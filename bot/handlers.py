from __future__ import annotations

import logging
from decimal import Decimal

from telegram import Update
from telegram.ext import ContextTypes
from web3 import AsyncWeb3

from bot.config import Config
from bot.db import check_cooldown, check_daily_cap, record_drip
from bot.eth import get_faucet_balance, send_drip

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

    # Check daily cap
    ok, cap_msg = await check_daily_cap(pool, config.daily_cap_wei)
    if not ok:
        await update.message.reply_text(cap_msg)
        return

    # Check cooldown
    ok, wait_msg = await check_cooldown(pool, telegram_uid, address, config.cooldown_hours)
    if not ok:
        await update.message.reply_text(wait_msg)
        return

    # Check faucet balance
    balance_wei = await get_faucet_balance(w3, config.faucet_address)
    if balance_wei < config.drip_amount_wei:
        await update.message.reply_text("Faucet is empty. Please try again later.")
        return

    # Send transaction
    try:
        tx_hash = await send_drip(
            w3, config.faucet_private_key, address, config.drip_amount_wei, config.chain_id
        )
    except Exception:
        logger.exception("Failed to send drip transaction")
        await update.message.reply_text("Transaction failed. Please try again later.")
        return

    # Record in DB
    await record_drip(pool, telegram_uid, address, tx_hash, config.drip_amount_wei)

    drip_eth = Decimal(config.drip_amount_wei) / Decimal(10**18)
    await update.message.reply_text(
        f"Sent {drip_eth} Sepolia ETH!\n"
        f"Tx: https://sepolia.etherscan.io/tx/0x{tx_hash}"
    )


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Unhandled exception", exc_info=context.error)
    if isinstance(update, Update) and update.message:
        await update.message.reply_text("An unexpected error occurred. Please try again later.")
