from __future__ import annotations

from web3 import AsyncWeb3, AsyncHTTPProvider
from eth_account import Account


def create_web3(rpc_url: str) -> AsyncWeb3:
    return AsyncWeb3(AsyncHTTPProvider(rpc_url))


async def get_faucet_balance(w3: AsyncWeb3, address: str) -> int:
    return await w3.eth.get_balance(address)


async def send_drip(
    w3: AsyncWeb3,
    private_key: str,
    to_address: str,
    amount_wei: int,
    chain_id: int,
) -> str:
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
    tx_hash = await w3.eth.send_raw_transaction(signed.raw_transaction)
    return tx_hash.hex()
