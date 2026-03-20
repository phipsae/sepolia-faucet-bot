from __future__ import annotations

import os
from dataclasses import dataclass
from decimal import Decimal

from dotenv import load_dotenv
from eth_account import Account

load_dotenv()


@dataclass(frozen=True)
class Config:
    telegram_bot_token: str
    faucet_private_key: str
    faucet_address: str
    eth_rpc_url: str
    database_url: str
    drip_amount_wei: int
    cooldown_hours: int
    daily_cap_wei: int
    chain_id: int

    @classmethod
    def from_env(cls) -> Config:
        telegram_bot_token = _require("TELEGRAM_BOT_TOKEN")
        faucet_private_key = _require("FAUCET_PRIVATE_KEY")
        eth_rpc_url = _require("ETH_RPC_URL")
        database_url = _require("DATABASE_URL")

        # Validate private key and derive address
        account = Account.from_key(faucet_private_key)
        faucet_address = account.address

        drip_eth = Decimal(os.getenv("DRIP_AMOUNT_ETH", "0.1"))
        drip_amount_wei = int(drip_eth * Decimal(10**18))

        cooldown_hours = int(os.getenv("COOLDOWN_HOURS", "24"))
        daily_cap_eth = Decimal(os.getenv("DAILY_CAP_ETH", "10"))
        daily_cap_wei = int(daily_cap_eth * Decimal(10**18))
        chain_id = int(os.getenv("SEPOLIA_CHAIN_ID", "11155111"))

        return cls(
            telegram_bot_token=telegram_bot_token,
            faucet_private_key=faucet_private_key,
            faucet_address=faucet_address,
            eth_rpc_url=eth_rpc_url,
            database_url=database_url,
            drip_amount_wei=drip_amount_wei,
            cooldown_hours=cooldown_hours,
            daily_cap_wei=daily_cap_wei,
            chain_id=chain_id,
        )

    def __repr__(self) -> str:
        return (
            f"Config(faucet_address={self.faucet_address!r}, "
            f"chain_id={self.chain_id}, "
            f"drip_amount_wei={self.drip_amount_wei}, "
            f"cooldown_hours={self.cooldown_hours}, "
            f"private_key=****)"
        )


def _require(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value
