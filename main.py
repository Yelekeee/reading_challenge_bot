"""
Entry point for the Daily Reading Challenge Bot.

Start-up sequence:
  1. Connect to SQLite database (creates schema on first run).
  2. Register middleware (group registration + pending-username resolver).
  3. Register routers (admin, participant, poll-answer, misc).
  4. Restore APScheduler jobs for every group that has an active challenge.
  5. Start aiogram long-polling (swap for webhook in production).

IMPORTANT — Telegram privacy mode:
  The bot MUST have privacy mode DISABLED in @BotFather (/setprivacy → Disable)
  so it receives all group messages, not just commands.  This is required for
  the passive username-resolution middleware to work.
"""

import asyncio
import logging

import pytz
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import (
    BotCommand,
    BotCommandScopeAllGroupChats,
    BotCommandScopeAllPrivateChats,
    ChatMemberUpdated,
    Message,
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from config import BOT_TOKEN, DATABASE_PATH, TIMEZONE
from db import Database
from handlers.admin import router as admin_router
from handlers.participant import router as participant_router
from handlers.poll import router as poll_router
from jobs import schedule_group_jobs
from middleware import GroupRegistrationMiddleware

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

TZ = pytz.timezone(TIMEZONE)


# ---------------------------------------------------------------------------
# Bot command menus
# ---------------------------------------------------------------------------

ADMIN_COMMANDS = [
    BotCommand(command="challenge_start",    description="Start the daily poll schedule"),
    BotCommand(command="challenge_stop",     description="Pause the daily poll schedule"),
    BotCommand(command="set_time",           description="Change poll time: /set_time HH:MM"),
    BotCommand(command="add",                description="Add participant (reply or @username)"),
    BotCommand(command="remove",             description="Remove participant (reply or @username)"),
    BotCommand(command="participants",       description="List active participants"),
    BotCommand(command="weekly_summary_now", description="Post current-week preview now"),
]

PARTICIPANT_COMMANDS = [
    BotCommand(command="join",        description="Join the reading challenge"),
    BotCommand(command="leave",       description="Leave the challenge"),
    BotCommand(command="today",       description="Today's vote status"),
    BotCommand(command="stats",       description="Your weekly + all-time stats"),
    BotCommand(command="leaderboard", description="Current-week leaderboard"),
]


async def set_commands(bot: Bot) -> None:
    # Group chats: show participant commands to everyone
    await bot.set_my_commands(
        PARTICIPANT_COMMANDS + ADMIN_COMMANDS,
        scope=BotCommandScopeAllGroupChats(),
    )
    # Private chats: minimal info
    await bot.set_my_commands(
        [BotCommand(command="start", description="About this bot")],
        scope=BotCommandScopeAllPrivateChats(),
    )


# ---------------------------------------------------------------------------
# Misc handlers (registered directly on the dispatcher)
# ---------------------------------------------------------------------------

async def on_private_start(message: Message) -> None:
    await message.answer(
        "👋 Hi! I'm the <b>Daily Reading Challenge</b> bot.\n\n"
        "I'm designed for groups. Add me to your reading group and an admin "
        "can run /challenge_start to kick things off.\n\n"
        "<b>How it works:</b>\n"
        "• Every day at 20:00 (Asia/Almaty) I post a poll: \u201cDid you read 30 min?\u201d\n"
        "• I track each participant's ✅/❌ votes.\n"
        "• Every Monday I post a weekly leaderboard.\n\n"
        "<b>⚠️ Important:</b> Make sure my privacy mode is <b>disabled</b> in "
        "@BotFather so I can see all group messages.",
        parse_mode="HTML",
    )


async def on_bot_added(event: ChatMemberUpdated, db: Database) -> None:
    """Register group when bot is added; deactivate when kicked."""
    new_status = event.new_chat_member.status
    chat = event.chat

    if chat.type not in ("group", "supergroup"):
        return

    if new_status in ("member", "administrator"):
        await db.get_or_create_group(chat.id, chat.title or "")
        logger.info("Bot added to group %s (%s)", chat.id, chat.title)
    elif new_status in ("kicked", "left"):
        await db.deactivate_group(chat.id)
        logger.info("Bot removed from group %s", chat.id)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    # --- Infrastructure ---
    db = Database(DATABASE_PATH)
    await db.connect()

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )

    dp = Dispatcher()

    scheduler = AsyncIOScheduler(timezone=TZ)

    # --- Middleware (applied to ALL messages) ---
    dp.message.middleware(GroupRegistrationMiddleware())

    # --- Routers ---
    dp.include_router(admin_router)
    dp.include_router(participant_router)
    dp.include_router(poll_router)

    # --- Misc handlers on the dispatcher directly ---
    dp.message.register(
        on_private_start,
        Command("start"),
        F.chat.type == "private",
    )
    dp.my_chat_member.register(on_bot_added)

    # --- Restore scheduler jobs for active challenges ---
    active = await db.get_all_active_challenges()
    for row in active:
        schedule_group_jobs(scheduler, row["group_id"], row["poll_time"], bot, db)
    logger.info("Restored %d active challenge(s) from database.", len(active))

    scheduler.start()

    # --- Set bot command menus ---
    await set_commands(bot)

    # --- Start polling ---
    logger.info("Bot started. Polling for updates…")
    try:
        await dp.start_polling(
            bot,
            db=db,
            scheduler=scheduler,
            allowed_updates=dp.resolve_used_update_types(),
        )
    finally:
        scheduler.shutdown(wait=False)
        await db.close()
        await bot.session.close()
        logger.info("Bot shut down cleanly.")


if __name__ == "__main__":
    asyncio.run(main())
