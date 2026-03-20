from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
import sys
import types
import unittest
from unittest.mock import AsyncMock, patch

from web3.exceptions import TransactionNotFound

if "asyncpg" not in sys.modules:
    fake_asyncpg = types.ModuleType("asyncpg")
    fake_asyncpg.Pool = object
    fake_asyncpg.Connection = object
    fake_asyncpg.Record = dict

    async def _unused_create_pool(*args, **kwargs):
        raise RuntimeError("create_pool should not be called in unit tests")

    fake_asyncpg.create_pool = _unused_create_pool
    sys.modules["asyncpg"] = fake_asyncpg

if "dotenv" not in sys.modules:
    fake_dotenv = types.ModuleType("dotenv")
    fake_dotenv.load_dotenv = lambda *args, **kwargs: None
    sys.modules["dotenv"] = fake_dotenv

import bot.db as db
import bot.handlers as handlers
from bot.config import Config
from bot.eth import PreparedDrip


def make_config() -> Config:
    return Config(
        telegram_bot_token="token",
        faucet_private_key="0x" + "1" * 64,
        faucet_address="0x" + "2" * 40,
        eth_rpc_url="http://localhost:8545",
        database_url="postgresql://localhost/faucet",
        drip_amount_wei=100,
        cooldown_hours=24,
        daily_cap_wei=1_000,
        pending_timeout_seconds=120,
        chain_id=11155111,
    )


class ProcessDripRequestTests(unittest.IsolatedAsyncioTestCase):
    async def test_success_path_reserves_and_marks_sent(self) -> None:
        conn = object()
        config = make_config()

        @asynccontextmanager
        async def fake_claim_lock(_pool):
            yield conn

        with (
            patch.object(handlers, "claim_lock", fake_claim_lock),
            patch.object(handlers, "reconcile_stale_pending", AsyncMock()),
            patch.object(handlers, "check_daily_cap", AsyncMock(return_value=(True, None))),
            patch.object(handlers, "check_cooldown", AsyncMock(return_value=(True, None))),
            patch.object(handlers, "get_faucet_balance", AsyncMock(return_value=config.drip_amount_wei)),
            patch.object(
                handlers,
                "prepare_drip",
                AsyncMock(return_value=PreparedDrip(raw_transaction=b"raw", tx_hash="abc123")),
            ),
            patch.object(handlers, "create_pending_drip", AsyncMock(return_value=17)),
            patch.object(handlers, "broadcast_drip", AsyncMock(return_value="abc123")),
            patch.object(handlers, "mark_drip_sent", AsyncMock()) as mark_drip_sent,
            patch.object(handlers, "mark_drip_failed", AsyncMock()) as mark_drip_failed,
        ):
            tx_hash, error_message = await handlers._process_drip_request(
                pool=object(),
                w3=object(),
                config=config,
                telegram_uid=123,
                address="0x" + "3" * 40,
            )

        self.assertEqual("abc123", tx_hash)
        self.assertIsNone(error_message)
        mark_drip_sent.assert_awaited_once_with(conn, 17)
        mark_drip_failed.assert_not_called()

    async def test_duplicate_pending_returns_pending_message(self) -> None:
        conn = object()
        config = make_config()

        @asynccontextmanager
        async def fake_claim_lock(_pool):
            yield conn

        with (
            patch.object(handlers, "claim_lock", fake_claim_lock),
            patch.object(handlers, "reconcile_stale_pending", AsyncMock()),
            patch.object(handlers, "check_daily_cap", AsyncMock(return_value=(True, None))),
            patch.object(
                handlers,
                "check_cooldown",
                AsyncMock(return_value=(False, db.PENDING_REQUEST_MSG)),
            ),
            patch.object(handlers, "prepare_drip", AsyncMock()) as prepare_drip,
        ):
            tx_hash, error_message = await handlers._process_drip_request(
                pool=object(),
                w3=object(),
                config=config,
                telegram_uid=123,
                address="0x" + "3" * 40,
            )

        self.assertIsNone(tx_hash)
        self.assertEqual(db.PENDING_REQUEST_MSG, error_message)
        prepare_drip.assert_not_called()

    async def test_broadcast_error_with_seen_transaction_marks_sent(self) -> None:
        conn = object()
        config = make_config()

        @asynccontextmanager
        async def fake_claim_lock(_pool):
            yield conn

        with (
            patch.object(handlers, "claim_lock", fake_claim_lock),
            patch.object(handlers, "reconcile_stale_pending", AsyncMock()),
            patch.object(handlers, "check_daily_cap", AsyncMock(return_value=(True, None))),
            patch.object(handlers, "check_cooldown", AsyncMock(return_value=(True, None))),
            patch.object(handlers, "get_faucet_balance", AsyncMock(return_value=config.drip_amount_wei)),
            patch.object(
                handlers,
                "prepare_drip",
                AsyncMock(return_value=PreparedDrip(raw_transaction=b"raw", tx_hash="abc123")),
            ),
            patch.object(handlers, "create_pending_drip", AsyncMock(return_value=17)),
            patch.object(handlers, "broadcast_drip", AsyncMock(side_effect=RuntimeError("boom"))),
            patch.object(handlers, "_tx_exists", AsyncMock(return_value=True)),
            patch.object(handlers, "mark_drip_sent", AsyncMock()) as mark_drip_sent,
            patch.object(handlers, "mark_drip_failed", AsyncMock()) as mark_drip_failed,
        ):
            tx_hash, error_message = await handlers._process_drip_request(
                pool=object(),
                w3=object(),
                config=config,
                telegram_uid=123,
                address="0x" + "3" * 40,
            )

        self.assertEqual("abc123", tx_hash)
        self.assertIsNone(error_message)
        mark_drip_sent.assert_awaited_once_with(conn, 17)
        mark_drip_failed.assert_not_called()

    async def test_broadcast_error_without_transaction_marks_failed(self) -> None:
        conn = object()
        config = make_config()

        @asynccontextmanager
        async def fake_claim_lock(_pool):
            yield conn

        with (
            patch.object(handlers, "claim_lock", fake_claim_lock),
            patch.object(handlers, "reconcile_stale_pending", AsyncMock()),
            patch.object(handlers, "check_daily_cap", AsyncMock(return_value=(True, None))),
            patch.object(handlers, "check_cooldown", AsyncMock(return_value=(True, None))),
            patch.object(handlers, "get_faucet_balance", AsyncMock(return_value=config.drip_amount_wei)),
            patch.object(
                handlers,
                "prepare_drip",
                AsyncMock(return_value=PreparedDrip(raw_transaction=b"raw", tx_hash="abc123")),
            ),
            patch.object(handlers, "create_pending_drip", AsyncMock(return_value=17)),
            patch.object(handlers, "broadcast_drip", AsyncMock(side_effect=RuntimeError("boom"))),
            patch.object(handlers, "_tx_exists", AsyncMock(return_value=False)),
            patch.object(handlers, "mark_drip_sent", AsyncMock()) as mark_drip_sent,
            patch.object(handlers, "mark_drip_failed", AsyncMock()) as mark_drip_failed,
        ):
            tx_hash, error_message = await handlers._process_drip_request(
                pool=object(),
                w3=object(),
                config=config,
                telegram_uid=123,
                address="0x" + "3" * 40,
            )

        self.assertIsNone(tx_hash)
        self.assertEqual("Transaction failed. Please try again later.", error_message)
        mark_drip_sent.assert_not_called()
        mark_drip_failed.assert_awaited_once()


class ReconcilePendingTests(unittest.IsolatedAsyncioTestCase):
    async def test_reconcile_stale_pending_marks_sent_and_failed(self) -> None:
        conn = object()
        stale_rows = [
            {"id": 1, "tx_hash": "seen"},
            {"id": 2, "tx_hash": "missing"},
        ]

        with (
            patch.object(handlers, "get_stale_pending_drips", AsyncMock(return_value=stale_rows)),
            patch.object(
                handlers,
                "get_transaction_by_hash",
                AsyncMock(side_effect=[{"hash": "seen"}, TransactionNotFound("missing")]),
            ),
            patch.object(handlers, "mark_drip_sent", AsyncMock()) as mark_drip_sent,
            patch.object(handlers, "mark_drip_failed", AsyncMock()) as mark_drip_failed,
        ):
            await handlers.reconcile_stale_pending(conn, w3=object(), pending_timeout_seconds=120)

        mark_drip_sent.assert_awaited_once_with(conn, 1)
        mark_drip_failed.assert_awaited_once_with(conn, 2, "stale_pending_no_tx")


class DbDecisionTests(unittest.IsolatedAsyncioTestCase):
    async def test_check_cooldown_blocks_recent_pending(self) -> None:
        conn = SimpleNamespace(
            fetchrow=AsyncMock(return_value={"created_at": datetime.now(timezone.utc)})
        )

        ok, message = await db.check_cooldown(
            conn,
            telegram_uid=123,
            wallet_addr="0xAbC",
            cooldown_hours=24,
            pending_timeout_seconds=120,
        )

        self.assertFalse(ok)
        self.assertEqual(db.PENDING_REQUEST_MSG, message)
        self.assertEqual(1, conn.fetchrow.await_count)

    async def test_check_cooldown_blocks_wallet_on_recent_sent(self) -> None:
        now = datetime.now(timezone.utc)
        conn = SimpleNamespace(
            fetchrow=AsyncMock(
                side_effect=[
                    None,
                    None,
                    None,
                    {"created_at": now - timedelta(minutes=30)},
                ]
            )
        )

        ok, message = await db.check_cooldown(
            conn,
            telegram_uid=123,
            wallet_addr="0xAbC",
            cooldown_hours=24,
            pending_timeout_seconds=120,
        )

        self.assertFalse(ok)
        self.assertIsNotNone(message)
        self.assertTrue(message.startswith("This wallet already requested a drip. Please wait 23h "))

    async def test_check_daily_cap_reserves_requested_amount(self) -> None:
        conn = SimpleNamespace(fetchrow=AsyncMock(return_value={"total": 950}))

        ok, message = await db.check_daily_cap(
            conn,
            daily_cap_wei=1_000,
            request_amount_wei=100,
            pending_timeout_seconds=120,
        )

        self.assertFalse(ok)
        self.assertEqual("Daily faucet cap reached. Please try again tomorrow.", message)


if __name__ == "__main__":
    unittest.main()
