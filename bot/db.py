from __future__ import annotations

from datetime import datetime, timezone

import asyncpg

DDL = """
CREATE TABLE IF NOT EXISTS drip_requests (
    id            BIGSERIAL    PRIMARY KEY,
    telegram_uid  BIGINT       NOT NULL,
    wallet_addr   TEXT         NOT NULL,
    tx_hash       TEXT         NOT NULL,
    amount_wei    NUMERIC      NOT NULL,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_drip_telegram_uid ON drip_requests (telegram_uid, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_drip_wallet_addr  ON drip_requests (wallet_addr, created_at DESC);
"""


async def init_db(database_url: str) -> asyncpg.Pool:
    pool = await asyncpg.create_pool(database_url, min_size=1, max_size=5)
    async with pool.acquire() as conn:
        await conn.execute(DDL)
    return pool


async def close_db(pool: asyncpg.Pool) -> None:
    await pool.close()


async def check_cooldown(
    pool: asyncpg.Pool,
    telegram_uid: int,
    wallet_addr: str,
    cooldown_hours: int,
) -> tuple[bool, str | None]:
    """Return (True, None) if allowed, or (False, wait_message) if on cooldown."""
    now = datetime.now(timezone.utc)
    async with pool.acquire() as conn:
        # Check by telegram user ID
        row = await conn.fetchrow(
            """
            SELECT created_at FROM drip_requests
            WHERE telegram_uid = $1
            ORDER BY created_at DESC LIMIT 1
            """,
            telegram_uid,
        )
        if row and (msg := _cooldown_msg(row["created_at"], now, cooldown_hours, "Your Telegram account")):
            return False, msg

        # Check by wallet address
        row = await conn.fetchrow(
            """
            SELECT created_at FROM drip_requests
            WHERE wallet_addr = $1
            ORDER BY created_at DESC LIMIT 1
            """,
            wallet_addr.lower(),
        )
        if row and (msg := _cooldown_msg(row["created_at"], now, cooldown_hours, "This wallet")):
            return False, msg

    return True, None


def _cooldown_msg(
    last_request: datetime, now: datetime, cooldown_hours: int, subject: str
) -> str | None:
    elapsed = now - last_request.replace(tzinfo=timezone.utc)
    from datetime import timedelta

    limit = timedelta(hours=cooldown_hours)
    if elapsed < limit:
        remaining = limit - elapsed
        hours, rem = divmod(int(remaining.total_seconds()), 3600)
        minutes = rem // 60
        return f"{subject} already requested a drip. Please wait {hours}h {minutes}m."
    return None


async def check_daily_cap(pool: asyncpg.Pool, daily_cap_wei: int) -> tuple[bool, str | None]:
    """Return (True, None) if under cap, or (False, message) if daily cap reached."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT COALESCE(SUM(amount_wei), 0) AS total
            FROM drip_requests
            WHERE created_at > now() - interval '24 hours'
            """,
        )
        total = int(row["total"])
        if total >= daily_cap_wei:
            return False, "Daily faucet cap reached. Please try again tomorrow."
    return True, None


async def record_drip(
    pool: asyncpg.Pool,
    telegram_uid: int,
    wallet_addr: str,
    tx_hash: str,
    amount_wei: int,
) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO drip_requests (telegram_uid, wallet_addr, tx_hash, amount_wei)
            VALUES ($1, $2, $3, $4)
            """,
            telegram_uid,
            wallet_addr.lower(),
            tx_hash,
            amount_wei,
        )
