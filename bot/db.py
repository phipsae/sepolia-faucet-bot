from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

import asyncpg

CLAIM_LOCK_KEY = 42_4242
PENDING_REQUEST_MSG = "Request already being processed. Please wait a minute and try again."

DDL = """
CREATE TABLE IF NOT EXISTS drip_requests (
    id            BIGSERIAL    PRIMARY KEY,
    telegram_uid  BIGINT       NOT NULL,
    wallet_addr   TEXT         NOT NULL,
    tx_hash       TEXT         NOT NULL,
    amount_wei    NUMERIC      NOT NULL,
    status        TEXT         NOT NULL DEFAULT 'pending',
    error_message TEXT,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT drip_requests_status_check CHECK (status IN ('pending', 'sent', 'failed'))
);
CREATE INDEX IF NOT EXISTS idx_drip_telegram_uid ON drip_requests (telegram_uid, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_drip_wallet_addr  ON drip_requests (wallet_addr, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_drip_status_created_at ON drip_requests (status, created_at DESC);
"""


async def init_db(database_url: str) -> asyncpg.Pool:
    pool = await asyncpg.create_pool(database_url, min_size=1, max_size=5)
    async with pool.acquire() as conn:
        await conn.execute(DDL)
        await _migrate_schema(conn)
    return pool


async def close_db(pool: asyncpg.Pool) -> None:
    await pool.close()


async def _migrate_schema(conn: asyncpg.Connection) -> None:
    await conn.execute("ALTER TABLE drip_requests ADD COLUMN IF NOT EXISTS status TEXT")
    await conn.execute("ALTER TABLE drip_requests ADD COLUMN IF NOT EXISTS error_message TEXT")
    await conn.execute("UPDATE drip_requests SET status = 'sent' WHERE status IS NULL")
    await conn.execute("ALTER TABLE drip_requests ALTER COLUMN status SET DEFAULT 'pending'")
    await conn.execute("ALTER TABLE drip_requests ALTER COLUMN status SET NOT NULL")
    await conn.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conname = 'drip_requests_status_check'
                  AND conrelid = 'drip_requests'::regclass
            ) THEN
                ALTER TABLE drip_requests
                ADD CONSTRAINT drip_requests_status_check
                CHECK (status IN ('pending', 'sent', 'failed'));
            END IF;
        END $$;
        """
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_drip_status_created_at ON drip_requests (status, created_at DESC)"
    )


@asynccontextmanager
async def claim_lock(pool: asyncpg.Pool):
    conn = await pool.acquire()
    try:
        await conn.fetchval("SELECT pg_advisory_lock($1)", CLAIM_LOCK_KEY)
        yield conn
    finally:
        try:
            await conn.fetchval("SELECT pg_advisory_unlock($1)", CLAIM_LOCK_KEY)
        finally:
            await pool.release(conn)


async def check_cooldown(
    conn: asyncpg.Connection,
    telegram_uid: int,
    wallet_addr: str,
    cooldown_hours: int,
    pending_timeout_seconds: int,
) -> tuple[bool, str | None]:
    """Return (True, None) if allowed, or (False, wait_message) if on cooldown."""
    now = datetime.now(timezone.utc)
    wallet_addr = wallet_addr.lower()

    row = await conn.fetchrow(
        """
        SELECT created_at FROM drip_requests
        WHERE telegram_uid = $1
          AND status = 'pending'
          AND created_at > now() - ($2 * interval '1 second')
        ORDER BY created_at DESC LIMIT 1
        """,
        telegram_uid,
        pending_timeout_seconds,
    )
    if row:
        return False, PENDING_REQUEST_MSG

    row = await conn.fetchrow(
        """
        SELECT created_at FROM drip_requests
        WHERE wallet_addr = $1
          AND status = 'pending'
          AND created_at > now() - ($2 * interval '1 second')
        ORDER BY created_at DESC LIMIT 1
        """,
        wallet_addr,
        pending_timeout_seconds,
    )
    if row:
        return False, PENDING_REQUEST_MSG

    row = await conn.fetchrow(
        """
        SELECT created_at FROM drip_requests
        WHERE telegram_uid = $1
          AND status = 'sent'
        ORDER BY created_at DESC LIMIT 1
        """,
        telegram_uid,
    )
    if row and (msg := _cooldown_msg(row["created_at"], now, cooldown_hours, "Your Telegram account")):
        return False, msg

    row = await conn.fetchrow(
        """
        SELECT created_at FROM drip_requests
        WHERE wallet_addr = $1
          AND status = 'sent'
        ORDER BY created_at DESC LIMIT 1
        """,
        wallet_addr,
    )
    if row and (msg := _cooldown_msg(row["created_at"], now, cooldown_hours, "This wallet")):
        return False, msg

    return True, None


def _cooldown_msg(
    last_request: datetime, now: datetime, cooldown_hours: int, subject: str
) -> str | None:
    if last_request.tzinfo is None:
        last_request = last_request.replace(tzinfo=timezone.utc)
    else:
        last_request = last_request.astimezone(timezone.utc)
    elapsed = now - last_request

    limit = timedelta(hours=cooldown_hours)
    if elapsed < limit:
        remaining = limit - elapsed
        hours, rem = divmod(int(remaining.total_seconds()), 3600)
        minutes = rem // 60
        return f"{subject} already requested a drip. Please wait {hours}h {minutes}m."
    return None


async def check_daily_cap(
    conn: asyncpg.Connection,
    daily_cap_wei: int,
    request_amount_wei: int,
    pending_timeout_seconds: int,
) -> tuple[bool, str | None]:
    """Return (True, None) if under cap, or (False, message) if daily cap reached."""
    row = await conn.fetchrow(
        """
        SELECT COALESCE(SUM(amount_wei), 0) AS total
        FROM drip_requests
        WHERE created_at > now() - interval '24 hours'
          AND (
                status = 'sent'
                OR (
                    status = 'pending'
                    AND created_at > now() - ($1 * interval '1 second')
                )
          )
        """,
        pending_timeout_seconds,
    )
    total = int(row["total"])
    if total + request_amount_wei > daily_cap_wei:
        return False, "Daily faucet cap reached. Please try again tomorrow."
    return True, None


async def create_pending_drip(
    conn: asyncpg.Connection,
    telegram_uid: int,
    wallet_addr: str,
    tx_hash: str,
    amount_wei: int,
) -> int:
    return await conn.fetchval(
        """
        INSERT INTO drip_requests (telegram_uid, wallet_addr, tx_hash, amount_wei, status)
        VALUES ($1, $2, $3, $4, 'pending')
        RETURNING id
        """,
        telegram_uid,
        wallet_addr.lower(),
        tx_hash,
        amount_wei,
    )


async def mark_drip_sent(conn: asyncpg.Connection, drip_id: int) -> None:
    await conn.execute(
        """
        UPDATE drip_requests
        SET status = 'sent', error_message = NULL
        WHERE id = $1
        """,
        drip_id,
    )


async def mark_drip_failed(conn: asyncpg.Connection, drip_id: int, error_message: str) -> None:
    await conn.execute(
        """
        UPDATE drip_requests
        SET status = 'failed', error_message = $2
        WHERE id = $1
        """,
        drip_id,
        error_message,
    )


async def get_stale_pending_drips(
    conn: asyncpg.Connection, pending_timeout_seconds: int
) -> list[asyncpg.Record]:
    return await conn.fetch(
        """
        SELECT id, tx_hash
        FROM drip_requests
        WHERE status = 'pending'
          AND created_at <= now() - ($1 * interval '1 second')
        ORDER BY created_at ASC
        """,
        pending_timeout_seconds,
    )
