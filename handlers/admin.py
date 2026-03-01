"""
Admin-only command handlers.

Commands:
  /challenge_start
  /challenge_stop
  /set_time HH:MM
  /add        (reply or @username)
  /remove     (reply or @username)
  /participants
  /weekly_summary_now
"""

import re
import logging

from aiogram import Router, Bot
from aiogram.filters import Command, CommandObject
from aiogram.types import Message
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from db import Database
from jobs import post_weekly_summary, remove_group_jobs, schedule_group_jobs, send_poll_reminder
from middleware import IsAdmin, IsGroup
from utils import format_mention, get_almaty_today, get_current_month_bounds

logger = logging.getLogger(__name__)
router = Router()


# ---------------------------------------------------------------------------
# /challenge_start
# ---------------------------------------------------------------------------

@router.message(Command("challenge_start"), IsGroup(), IsAdmin())
async def cmd_challenge_start(
    msg: Message,
    db: Database,
    scheduler: AsyncIOScheduler,
    bot: Bot,
) -> None:
    group_id = msg.chat.id
    settings = await db.get_settings(group_id)

    if not settings:
        await msg.reply("❌ Group not initialised yet. Try again in a moment.")
        return

    if settings["challenge_active"]:
        await msg.reply(
            f"ℹ️ Challenge is already running. "
            f"Daily poll at <b>{settings['poll_time']}</b> (Asia/Almaty).",
            parse_mode="HTML",
        )
        return

    await db.set_challenge_active(group_id, True)
    schedule_group_jobs(
        scheduler, group_id, settings["poll_time"], bot, db,
        reminder_time=settings["reminder_time"],
    )

    reminder_line = (
        f"\n⏰ Reminder at <b>{settings['reminder_time']}</b> for unvoted participants."
        if settings["reminder_time"] else ""
    )
    await msg.reply(
        f"✅ Daily reading challenge started!\n"
        f"📅 Poll at <b>{settings['poll_time']}</b> (Asia/Almaty) every day.{reminder_line}\n\n"
        f"Use /join to attend the poll and become a participant of this challenge.",
        parse_mode="HTML",
    )


# ---------------------------------------------------------------------------
# /challenge_stop
# ---------------------------------------------------------------------------

@router.message(Command("challenge_stop"), IsGroup(), IsAdmin())
async def cmd_challenge_stop(
    msg: Message,
    db: Database,
    scheduler: AsyncIOScheduler,
) -> None:
    group_id = msg.chat.id
    settings = await db.get_settings(group_id)

    if not settings or not settings["challenge_active"]:
        await msg.reply("ℹ️ Challenge is not running.")
        return

    await db.set_challenge_active(group_id, False)
    remove_group_jobs(scheduler, group_id)
    await msg.reply("⏸ Challenge paused. All data is preserved. Use /challenge_start to resume.")


# ---------------------------------------------------------------------------
# /set_time HH:MM
# ---------------------------------------------------------------------------

@router.message(Command("set_time"), IsGroup(), IsAdmin())
async def cmd_set_time(
    msg: Message,
    db: Database,
    scheduler: AsyncIOScheduler,
    bot: Bot,
    command: CommandObject,
) -> None:
    group_id = msg.chat.id
    raw = (command.args or "").strip()

    if not re.fullmatch(r"([01]?\d|2[0-3]):[0-5]\d", raw):
        await msg.reply("❌ Invalid format. Example: <code>/set_time 20:00</code>", parse_mode="HTML")
        return

    poll_time = raw
    settings = await db.get_settings(group_id)
    await db.set_poll_time(group_id, poll_time)

    if settings and settings["challenge_active"]:
        schedule_group_jobs(
            scheduler, group_id, poll_time, bot, db,
            reminder_time=settings["reminder_time"],
        )
        await msg.reply(
            f"✅ Poll time updated to <b>{poll_time}</b> (Asia/Almaty). Jobs rescheduled.",
            parse_mode="HTML",
        )
    else:
        await msg.reply(
            f"✅ Poll time set to <b>{poll_time}</b> (Asia/Almaty). "
            f"Start the challenge with /challenge_start.",
            parse_mode="HTML",
        )


# ---------------------------------------------------------------------------
# /set_reminder_time HH:MM
# ---------------------------------------------------------------------------

@router.message(Command("set_reminder_time"), IsGroup(), IsAdmin())
async def cmd_set_reminder_time(
    msg: Message,
    db: Database,
    scheduler: AsyncIOScheduler,
    bot: Bot,
    command: CommandObject,
) -> None:
    group_id = msg.chat.id
    raw = (command.args or "").strip()

    if not re.fullmatch(r"([01]?\d|2[0-3]):[0-5]\d", raw):
        await msg.reply(
            "❌ Invalid format. Example: <code>/set_reminder_time 22:00</code>",
            parse_mode="HTML",
        )
        return

    reminder_time = raw
    settings = await db.get_settings(group_id)
    await db.set_reminder_time(group_id, reminder_time)

    if settings and settings["challenge_active"]:
        schedule_group_jobs(
            scheduler, group_id, settings["poll_time"], bot, db,
            reminder_time=reminder_time,
        )
        await msg.reply(
            f"✅ Reminder time set to <b>{reminder_time}</b> (Asia/Almaty).\n"
            f"Participants who haven't voted by this time will be tagged.",
            parse_mode="HTML",
        )
    else:
        await msg.reply(
            f"✅ Reminder time set to <b>{reminder_time}</b> (Asia/Almaty). "
            f"Start the challenge with /challenge_start.",
            parse_mode="HTML",
        )


# ---------------------------------------------------------------------------
# /reminder_now  (manual trigger for testing)
# ---------------------------------------------------------------------------

@router.message(Command("reminder_now"), IsGroup(), IsAdmin())
async def cmd_reminder_now(msg: Message, db: Database, bot: Bot) -> None:
    await msg.reply("⏰ Sending reminder to unvoted participants…")
    await send_poll_reminder(msg.chat.id, bot, db)


# ---------------------------------------------------------------------------
# /add  (reply or @username)
# ---------------------------------------------------------------------------

@router.message(Command("add"), IsGroup(), IsAdmin())
async def cmd_add(
    msg: Message,
    db: Database,
    command: CommandObject,
) -> None:
    group_id = msg.chat.id

    # Case A: admin replies to a user's message
    if msg.reply_to_message and msg.reply_to_message.from_user:
        target = msg.reply_to_message.from_user
        if target.is_bot:
            await msg.reply("❌ Cannot add a bot as a participant.")
            return

        await db.upsert_participant(
            group_id, target.id, target.username, target.full_name
        )
        mention = format_mention(target.id, target.username, target.full_name)
        await msg.reply(f"✅ {mention} added to the challenge.", parse_mode="HTML")
        return

    # Case B: /add @username
    raw = (command.args or "").strip().lstrip("@")
    if not raw:
        await msg.reply(
            "Usage:\n"
            "• Reply to someone's message: <code>/add</code>\n"
            "• By username: <code>/add @username</code>",
            parse_mode="HTML",
        )
        return

    username = raw
    existing = await db.get_participant_by_username(group_id, username)
    if existing and existing["user_id"]:
        # We already have their user_id — activate directly
        await db.upsert_participant(
            group_id, existing["user_id"], username, existing["display_name"]
        )
        await msg.reply(f"✅ @{username} added to the challenge.")
    else:
        await db.add_pending_participant(group_id, username)
        await msg.reply(
            f"⏳ <b>@{username}</b> queued.\n"
            f"They'll be fully registered when they send any message in this group, "
            f"or they can use /join themselves.",
            parse_mode="HTML",
        )


# ---------------------------------------------------------------------------
# /remove  (reply or @username)
# ---------------------------------------------------------------------------

@router.message(Command("remove"), IsGroup(), IsAdmin())
async def cmd_remove(
    msg: Message,
    db: Database,
    command: CommandObject,
) -> None:
    group_id = msg.chat.id

    # Case A: reply
    if msg.reply_to_message and msg.reply_to_message.from_user:
        target = msg.reply_to_message.from_user
        removed = await db.deactivate_participant_by_user_id(group_id, target.id)
        name = format_mention(target.id, target.username, target.full_name)
        if removed:
            await msg.reply(f"✅ {name} removed from the challenge.", parse_mode="HTML")
        else:
            await msg.reply(f"❌ {name} is not an active participant.", parse_mode="HTML")
        return

    # Case B: @username
    raw = (command.args or "").strip().lstrip("@")
    if not raw:
        await msg.reply(
            "Usage:\n"
            "• Reply to someone's message: <code>/remove</code>\n"
            "• By username: <code>/remove @username</code>",
            parse_mode="HTML",
        )
        return

    removed = await db.deactivate_participant_by_username(group_id, raw)
    if removed:
        await msg.reply(f"✅ @{raw} removed from the challenge.")
    else:
        await msg.reply(f"❌ @{raw} is not an active participant.")


# ---------------------------------------------------------------------------
# /participants
# ---------------------------------------------------------------------------

@router.message(Command("participants"), IsGroup(), IsAdmin())
async def cmd_participants(msg: Message, db: Database) -> None:
    group_id = msg.chat.id
    rows = await db.get_active_participants(group_id)

    if not rows:
        await msg.reply(
            "No active participants yet.\n"
            "Add them with /add or they can use /join."
        )
        return

    lines = [f"👥 <b>Active Participants ({len(rows)})</b>\n"]
    has_pending = False
    for i, p in enumerate(rows, 1):
        mention = format_mention(p["user_id"], p["username"], p["display_name"])
        pending_tag = " ⏳" if p["pending"] else ""
        has_pending = has_pending or bool(p["pending"])
        lines.append(f"{i}. {mention}{pending_tag}")

    if has_pending:
        lines.append(
            "\n<i>⏳ = pending (user_id not yet resolved; "
            "they need to send a message or use /join)</i>"
        )

    await msg.reply("\n".join(lines), parse_mode="HTML")


# ---------------------------------------------------------------------------
# /addall  (@username1 @username2 ...)
# ---------------------------------------------------------------------------

@router.message(Command("addall"), IsGroup(), IsAdmin())
async def cmd_addall(
    msg: Message,
    db: Database,
    command: CommandObject,
) -> None:
    group_id = msg.chat.id
    raw = (command.args or "").strip()

    if not raw:
        await msg.reply(
            "Қолданылуы: <code>/addall @username1 @username2 ...</code>",
            parse_mode="HTML",
        )
        return

    usernames = [u.lstrip("@") for u in raw.split() if u.strip()]
    if not usernames:
        await msg.reply("❌ Username табылмады.")
        return

    added_lines = []
    pending_lines = []

    for username in usernames:
        existing = await db.get_participant_by_username(group_id, username)
        if existing and existing["user_id"]:
            await db.upsert_participant(
                group_id, existing["user_id"], username, existing["display_name"]
            )
            added_lines.append(f"✅ @{username}")
        else:
            await db.add_pending_participant(group_id, username)
            pending_lines.append(f"⏳ @{username}")

    lines = [f"👥 <b>{len(usernames)} мүше өңделді:</b>\n"]
    if added_lines:
        lines.append("Қосылды: " + ", ".join(added_lines))
    if pending_lines:
        lines.append("Күтілуде: " + ", ".join(pending_lines))
    if pending_lines:
        lines.append(
            "\n<i>⏳ — user_id әлі белгісіз. "
            "Топта хабар жіберсе немесе /join пайдаланса тіркеледі.</i>"
        )

    await msg.reply("\n".join(lines), parse_mode="HTML")


# ---------------------------------------------------------------------------
# /weekly_summary_now
# ---------------------------------------------------------------------------

@router.message(Command("weekly_summary_now"), IsGroup(), IsAdmin())
async def cmd_weekly_summary_now(msg: Message, db: Database, bot: Bot) -> None:
    await msg.reply("📊 Generating current-week preview…")
    await post_weekly_summary(msg.chat.id, bot, db, preview=True)


# ---------------------------------------------------------------------------
# /monthly_summary_now
# ---------------------------------------------------------------------------

@router.message(Command("monthly_summary_now"), IsGroup(), IsAdmin())
async def cmd_monthly_summary_now(msg: Message, db: Database, bot: Bot) -> None:
    group_id = msg.chat.id
    month_start, month_end = get_current_month_bounds()
    today = get_almaty_today()
    days_so_far = (today - month_start).days + 1

    rows = await db.get_monthly_leaderboard(
        group_id, month_start.isoformat(), month_end.isoformat()
    )
    if not rows:
        await msg.reply("No participants yet.")
        return

    medals = ["🥇", "🥈", "🥉"]
    lines = [
        f"📅 <b>{month_start.strftime('%B %Y')} — Reading Challenge</b>\n"
        f"Day {days_so_far} of {(month_end - month_start).days + 1}\n"
    ]
    for i, p in enumerate(rows):
        medal = medals[i] if i < 3 else f"{i + 1}."
        mention = format_mention(p["user_id"], p["username"], p["display_name"])
        yes = p["yes_count"]
        rate = f"{yes / days_so_far * 100:.0f}%" if days_so_far > 0 else "0%"
        fire = " 🔥" if yes == days_so_far else ""
        warn = " ⚠️" if p["missed_count"] >= 4 else ""
        lines.append(f"{medal} {mention} — {yes}/{days_so_far} ({rate}){fire}{warn}")

    await bot.send_message(group_id, "\n".join(lines), parse_mode="HTML")
