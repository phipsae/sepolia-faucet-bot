import logging

from telegram.ext import ApplicationBuilder, CommandHandler

from bot.config import Config
from bot.db import init_db, close_db
from bot.eth import create_web3
from bot.handlers import start_handler, help_handler, balance_handler, drip_handler, error_handler

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


async def post_init(application) -> None:
    config: Config = application.bot_data["config"]
    application.bot_data["pool"] = await init_db(config.database_url)
    application.bot_data["w3"] = create_web3(config.eth_rpc_url)
    logger.info("Bot initialized — faucet address: %s", config.faucet_address)


async def post_shutdown(application) -> None:
    pool = application.bot_data.get("pool")
    if pool:
        await close_db(pool)
        logger.info("Database pool closed")


def main() -> None:
    config = Config.from_env()
    logger.info("Loaded config: %s", config)

    app = (
        ApplicationBuilder()
        .token(config.telegram_bot_token)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )
    app.bot_data["config"] = config

    app.add_handler(CommandHandler("start", start_handler))
    app.add_handler(CommandHandler("help", help_handler))
    app.add_handler(CommandHandler("balance", balance_handler))
    app.add_handler(CommandHandler("drip", drip_handler))
    app.add_error_handler(error_handler)

    logger.info("Starting bot polling…")
    app.run_polling()


if __name__ == "__main__":
    main()
