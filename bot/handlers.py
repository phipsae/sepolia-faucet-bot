from __future__ import annotations

import logging
from decimal import Decimal

from telegram import Update
from telegram.ext import ContextTypes
from web3 import AsyncWeb3
from web3.exceptions import TransactionNotFound

from bot.config import Config
from bot.db import (
    check_cooldown,
    check_daily_cap,
    claim_lock,
    create_pending_drip,
    get_stale_pending_drips,
    mark_drip_failed,
    mark_drip_sent,
)
from bot.eth import broadcast_drip, get_faucet_balance, get_transaction_by_hash, prepare_drip

logger = logging.getLogger(__name__)

WELCOME_TEXT = (
    "Welcome to the Sepolia ETH Faucet!\n\n"
    "Commands:\n"
    "/drip <address> — Request 0.1 Sepolia ETH\n"
    "/balance — Show faucet wallet balance\n"
    "/help — Show this message"
)


def _get(context: ContextTypes.DEFAULT_TYPE) -> tuple:
    bd = context.application.bot_data
    return bd["pool"], bd["w3"], bd["config"]


async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(WELCOME_TEXT)


async def help_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(WELCOME_TEXT)


async def balance_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    pool, w3, config = _get(context)
    balance_wei = await get_faucet_balance(w3, config.faucet_address)
    balance_eth = Decimal(balance_wei) / Decimal(10**18)
    await update.message.reply_text(
        f"Faucet address: {config.faucet_address}\n"
        f"Balance: {balance_eth:.6f} ETH"
    )


async def drip_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    pool, w3, config = _get(context)

    if not context.args:
        await update.message.reply_text("Usage: /drip <ethereum_address>")
        return

    address = context.args[0]

    # Validate address
    if not AsyncWeb3.is_address(address):
        await update.message.reply_text("Invalid Ethereum address. Please check and try again.")
        return

    telegram_uid = update.effective_user.id

    tx_hash, error_message = await _process_drip_request(
        pool, w3, config, telegram_uid, address
    )
    if error_message:
        await update.message.reply_text(error_message)
        return

    drip_eth = Decimal(config.drip_amount_wei) / Decimal(10**18)
    await update.message.reply_text(
        f"Sent {drip_eth} Sepolia ETH!\n"
        f"Tx: https://sepolia.etherscan.io/tx/0x{tx_hash}"
    )


async def _process_drip_request(
    pool,
    w3: AsyncWeb3,
    config: Config,
    telegram_uid: int,
    address: str,
) -> tuple[str | None, str | None]:
    async with claim_lock(pool) as conn:
        await reconcile_stale_pending(conn, w3, config.pending_timeout_seconds)

        ok, cap_msg = await check_daily_cap(
            conn,
            config.daily_cap_wei,
            config.drip_amount_wei,
            config.pending_timeout_seconds,
        )
        if not ok:
            return None, cap_msg

        ok, wait_msg = await check_cooldown(
            conn,
            telegram_uid,
            address,
            config.cooldown_hours,
            config.pending_timeout_seconds,
        )
        if not ok:
            return None, wait_msg

        balance_wei = await get_faucet_balance(w3, config.faucet_address)
        if balance_wei < config.drip_amount_wei:
            return None, "Faucet is empty. Please try again later."

        prepared = await prepare_drip(
            w3, config.faucet_private_key, address, config.drip_amount_wei, config.chain_id
        )
        drip_id = await create_pending_drip(
            conn, telegram_uid, address, prepared.tx_hash, config.drip_amount_wei
        )

        try:
            broadcast_hash = await broadcast_drip(w3, prepared.raw_transaction)
        except Exception as exc:
            logger.exception("Failed to broadcast drip transaction")
            tx_seen = await _tx_exists(w3, prepared.tx_hash)
            if tx_seen:
                await mark_drip_sent(conn, drip_id)
                return prepared.tx_hash, None

            if tx_seen is False:
                await mark_drip_failed(conn, drip_id, _format_error(exc))
            return None, "Transaction failed. Please try again later."

        if broadcast_hash != prepared.tx_hash:
            logger.warning(
                "Broadcast tx hash mismatch; prepared=%s broadcast=%s",
                prepared.tx_hash,
                broadcast_hash,
            )
        await mark_drip_sent(conn, drip_id)
        return prepared.tx_hash, None


async def reconcile_stale_pending(conn, w3: AsyncWeb3, pending_timeout_seconds: int) -> None:
    stale_rows = await get_stale_pending_drips(conn, pending_timeout_seconds)
    for row in stale_rows:
        try:
            await get_transaction_by_hash(w3, row["tx_hash"])
        except TransactionNotFound:
            await mark_drip_failed(conn, row["id"], "stale_pending_no_tx")
        except Exception:
            logger.exception("Failed to reconcile pending drip %s", row["id"])
        else:
            await mark_drip_sent(conn, row["id"])


async def _tx_exists(w3: AsyncWeb3, tx_hash: str) -> bool | None:
    try:
        await get_transaction_by_hash(w3, tx_hash)
    except TransactionNotFound:
        return False
    except Exception:
        logger.exception("Failed to look up transaction %s after broadcast error", tx_hash)
        return None
    return True


def _format_error(exc: Exception) -> str:
    message = str(exc).strip() or exc.__class__.__name__
    return message[:200]


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Unhandled exception", exc_info=context.error)
    if isinstance(update, Update) and update.message:
        await update.message.reply_text("An unexpected error occurred. Please try again later.")
