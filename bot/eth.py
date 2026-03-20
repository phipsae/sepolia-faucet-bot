from __future__ import annotations

from dataclasses import dataclass

from web3 import AsyncWeb3, AsyncHTTPProvider
from eth_account import Account


@dataclass(frozen=True)
class PreparedDrip:
    raw_transaction: bytes
    tx_hash: str


def create_web3(rpc_url: str) -> AsyncWeb3:
    return AsyncWeb3(AsyncHTTPProvider(rpc_url))


async def get_faucet_balance(w3: AsyncWeb3, address: str) -> int:
    return await w3.eth.get_balance(address)


async def prepare_drip(
    w3: AsyncWeb3,
    private_key: str,
    to_address: str,
    amount_wei: int,
    chain_id: int,
) -> PreparedDrip:
    account = Account.from_key(private_key)
    nonce = await w3.eth.get_transaction_count(account.address)

    tx = {
        "to": w3.to_checksum_address(to_address),
        "value": amount_wei,
        "gas": 21_000,
        "maxFeePerGas": await w3.eth.gas_price * 2,
        "maxPriorityFeePerGas": await w3.eth.max_priority_fee,
        "nonce": nonce,
        "chainId": chain_id,
        "type": 2,
    }

    signed = Account.sign_transaction(tx, private_key)
    return PreparedDrip(raw_transaction=bytes(signed.raw_transaction), tx_hash=signed.hash.hex())


async def broadcast_drip(w3: AsyncWeb3, raw_transaction: bytes) -> str:
    tx_hash = await w3.eth.send_raw_transaction(raw_transaction)
    return tx_hash.hex()


async def get_transaction_by_hash(w3: AsyncWeb3, tx_hash: str):
    if not tx_hash.startswith("0x"):
        tx_hash = f"0x{tx_hash}"
    return await w3.eth.get_transaction(tx_hash)
