"""
Scheduler jobs:
  post_daily_poll        — posts the daily non-anonymous poll at the configured time
  snapshot_daily_results — records each participant's final vote status at 23:59
  post_weekly_summary    — posts the weekly leaderboard every Monday at 09:00
                           and snapshots weekly_results (idempotent)

schedule_group_jobs / remove_group_jobs manage APScheduler entries per group.
"""

import asyncio
import logging
from datetime import date

import pytz
from aiogram import Bot
from aiogram.exceptions import TelegramAPIError
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from config import TIMEZONE
from db import Database
from utils import (
    format_mention,
    get_almaty_today,
    get_current_week_bounds,
    get_prev_week_bounds,
    make_poll_link,
)

logger = logging.getLogger(__name__)
TZ = pytz.timezone(TIMEZONE)


# ---------------------------------------------------------------------------
# Job: daily poll
# ---------------------------------------------------------------------------

async def post_daily_poll(group_id: int, bot: Bot, db: Database) -> None:
    today = get_almaty_today().isoformat()

    # --- Idempotency: reserve slot before touching Telegram ---
    poll_db_id = await db.try_create_poll_slot(group_id, today)
    if poll_db_id is None:
        logger.info("poll already posted for group=%s date=%s — skipping", group_id, today)
        return

    participants = await db.get_active_participants(group_id)

    # Build mention string (all mentions in one message — anti-spam)
    mention_parts = [
        format_mention(p["user_id"], p["username"], p["display_name"])
        for p in participants
    ]
    mentions_line = " ".join(mention_parts) if mention_parts else "everyone"

    try:
        await bot.send_message(
            chat_id=group_id,
            text=(
                f"📖 <b>Reading time!</b>\n\n"
                f"{mentions_line}\n\n"
                f"Vote in the poll below 👇"
            ),
            parse_mode="HTML",
        )

        await asyncio.sleep(0.5)  # small buffer before poll

        poll_msg = await bot.send_poll(
            chat_id=group_id,
            question="Did you read 30 minutes today?",
            options=["✅ Yes", "❌ No"],
            is_anonymous=False,         # REQUIRED for poll_answer updates
            allows_multiple_answers=False,
        )

        # Persist Telegram IDs so we can correlate poll_answer updates
        await db.update_poll_telegram_ids(
            group_id, today,
            poll_msg.poll.id,
            poll_msg.message_id,
        )

        # Attempt to pin the poll (non-critical — bot may lack permission)
        try:
            await bot.pin_chat_message(
                group_id, poll_msg.message_id, disable_notification=True
            )
        except TelegramAPIError:
            pass

        logger.info("Daily poll posted for group=%s date=%s", group_id, today)

    except TelegramAPIError as exc:
        logger.error(
            "Failed to post daily poll for group=%s: %s", group_id, exc
        )
        # Leave the slot reserved so we don't retry and double-post on restart.
        # Admins will notice the missing poll.


# ---------------------------------------------------------------------------
# Job: snapshot daily results at 23:59
# ---------------------------------------------------------------------------

async def snapshot_daily_results(group_id: int, bot: Bot, db: Database) -> None:
    today = get_almaty_today().isoformat()
    poll = await db.get_poll_by_date(group_id, today)
    participants = await db.get_active_participants(group_id)

    for p in participants:
        pid = p["id"]
        uid = p["user_id"]

        if not uid or not poll or not poll["tg_poll_id"]:
            # Pending participant or no poll today
            status = "missed"
        else:
            vote = await db.get_vote(poll["id"], uid)
            if vote is None or vote["option_idx"] is None:
                status = "missed"
            elif vote["option_idx"] == 0:
                status = "yes"
            else:
                status = "no"

        await db.upsert_daily_result(group_id, pid, today, status)

    logger.info(
        "Snapshotted daily results for group=%s date=%s (%d participants)",
        group_id, today, len(participants),
    )


# ---------------------------------------------------------------------------
# Job: weekly summary (Monday 09:00)
# ---------------------------------------------------------------------------

async def post_weekly_summary(
    group_id: int,
    bot: Bot,
    db: Database,
    *,
    preview: bool = False,
) -> None:
    """
    Post the weekly summary.

    preview=False (default, scheduled run):
        Covers the *previous* week (Mon–Sun).
        Snapshots weekly_results; idempotent (skips if already posted).

    preview=True (admin /weekly_summary_now):
        Covers the *current* week so far.
        Does NOT snapshot or reset; safe to run any time.
    """
    if preview:
        week_start, week_end = get_current_week_bounds()
        week_end = min(week_end, get_almaty_today())   # cap at today
        heading = "📊 <b>Current-week preview</b>"
    else:
        week_start, week_end = get_prev_week_bounds()
        heading = f"📊 <b>Week of {week_start.strftime('%b %d')}–{week_end.strftime('%d, %Y')}</b>"

    week_start_str = week_start.isoformat()
    week_end_str = week_end.isoformat()

    # Idempotency for scheduled run
    if not preview and await db.check_weekly_result_exists(group_id, week_start_str):
        logger.info(
            "Weekly summary already posted for group=%s week=%s", group_id, week_start_str
        )
        return

    rows = await db.get_weekly_leaderboard(group_id, week_start_str, week_end_str)
    if not rows:
        logger.info("No participants for group=%s — skipping weekly summary", group_id)
        return

    total_days = (week_end - week_start).days + 1
    medals = ["🥇", "🥈", "🥉"]

    lines = [
        f"{heading} — Reading Challenge\n"
    ]

    for i, p in enumerate(rows):
        yes = p["yes_count"]
        rate = yes / total_days * 100
        medal = medals[i] if i < 3 else "•"
        mention = format_mention(p["user_id"], p["username"], p["display_name"])
        fire = " 🔥" if yes == total_days else ""
        warn = " ⚠️" if p["missed_count"] >= 4 else ""
        lines.append(f"{medal} {mention} — {yes}/{total_days} ({rate:.0f}%){fire}{warn}")

        # Snapshot for scheduled run only
        if not preview:
            await db.insert_weekly_result(
                group_id, p["id"], week_start_str,
                yes, p["no_count"], p["missed_count"],
                round(rate, 2), i + 1,
            )

    if not preview:
        lines.append("\n📅 New week starts today. Keep reading! 📚")
        lines.append("<i>Weekly stats reset. All-time stats preserved.</i>")
    else:
        lines.append(f"\n<i>Preview — week runs {week_start.strftime('%b %d')}–{week_end.strftime('%b %d')}</i>")

    await bot.send_message(
        group_id,
        "\n".join(lines),
        parse_mode="HTML",
    )
    logger.info(
        "Weekly summary posted for group=%s week=%s preview=%s",
        group_id, week_start_str, preview,
    )


# ---------------------------------------------------------------------------
# Scheduler management helpers
# ---------------------------------------------------------------------------

def schedule_group_jobs(
    scheduler: AsyncIOScheduler,
    group_id: int,
    poll_time: str,
    bot: Bot,
    db: Database,
) -> None:
    """Add (or replace) the three cron jobs for a group."""
    hour, minute = map(int, poll_time.split(":"))

    scheduler.add_job(
        post_daily_poll,
        CronTrigger(hour=hour, minute=minute, timezone=TZ),
        id=f"poll_{group_id}",
        args=[group_id, bot, db],
        replace_existing=True,
        misfire_grace_time=300,      # fire even if bot was down ≤5 min
    )
    scheduler.add_job(
        snapshot_daily_results,
        CronTrigger(hour=23, minute=59, timezone=TZ),
        id=f"snapshot_{group_id}",
        args=[group_id, bot, db],
        replace_existing=True,
        misfire_grace_time=120,
    )
    scheduler.add_job(
        post_weekly_summary,
        CronTrigger(day_of_week="mon", hour=9, minute=0, timezone=TZ),
        id=f"weekly_{group_id}",
        args=[group_id, bot, db],
        replace_existing=True,
        misfire_grace_time=300,
    )
    logger.info(
        "Scheduled jobs for group=%s at %s Asia/Almaty", group_id, poll_time
    )


def remove_group_jobs(scheduler: AsyncIOScheduler, group_id: int) -> None:
    """Remove all scheduler jobs for a group (on /challenge_stop)."""
    for prefix in ("poll", "snapshot", "weekly"):
        job_id = f"{prefix}_{group_id}"
        if scheduler.get_job(job_id):
            scheduler.remove_job(job_id)
    logger.info("Removed scheduler jobs for group=%s", group_id)
