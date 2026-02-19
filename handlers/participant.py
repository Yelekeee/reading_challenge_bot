"""
Participant-facing command handlers.

Commands (group-only, no admin required):
  /join       — self-enrol in the challenge
  /leave      — self-remove (history preserved)
  /today      — today's vote status + poll link
  /stats      — weekly + all-time breakdown
  /leaderboard — current-week standings
"""

import logging

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message

from db import Database
from middleware import IsGroup
from utils import (
    format_mention,
    get_almaty_today,
    get_current_week_bounds,
    make_poll_link,
)

logger = logging.getLogger(__name__)
router = Router()


# ---------------------------------------------------------------------------
# /join
# ---------------------------------------------------------------------------

@router.message(Command("join"), IsGroup())
async def cmd_join(msg: Message, db: Database) -> None:
    group_id = msg.chat.id
    user = msg.from_user

    existing = await db.get_participant_by_user_id(group_id, user.id)
    if existing and existing["active"]:
        await msg.reply("✅ You're already a participant in this challenge!")
        return

    await db.upsert_participant(group_id, user.id, user.username, user.full_name)
    await msg.reply(
        "✅ <b>You've joined the reading challenge!</b>\n\n"
        "You'll be tracked in the daily 20:00 poll.\n"
        "Read 30 minutes every day and vote ✅ Yes! 📚",
        parse_mode="HTML",
    )


# ---------------------------------------------------------------------------
# /leave
# ---------------------------------------------------------------------------

@router.message(Command("leave"), IsGroup())
async def cmd_leave(msg: Message, db: Database) -> None:
    group_id = msg.chat.id
    removed = await db.deactivate_participant_by_user_id(group_id, msg.from_user.id)
    if removed:
        await msg.reply(
            "👋 You've left the reading challenge.\n"
            "Your reading history is preserved. You can /join again any time."
        )
    else:
        await msg.reply("❌ You're not an active participant.")


# ---------------------------------------------------------------------------
# /today
# ---------------------------------------------------------------------------

@router.message(Command("today"), IsGroup())
async def cmd_today(msg: Message, db: Database) -> None:
    group_id = msg.chat.id
    user_id = msg.from_user.id
    today = get_almaty_today().isoformat()

    participant = await db.get_participant_by_user_id(group_id, user_id)
    if not participant or not participant["active"]:
        await msg.reply("❌ You're not a participant. Use /join to join the challenge.")
        return

    poll = await db.get_poll_by_date(group_id, today)

    # Determine vote status
    if not poll or not poll["tg_poll_id"]:
        vote_line = "⏰ No poll posted yet today."
        link_line = ""
    else:
        vote_row = await db.get_today_vote_for_user(group_id, user_id, today)
        if vote_row is None or vote_row["option_idx"] is None:
            vote_line = "🗳 You haven't voted yet — check the poll below!"
        elif vote_row["option_idx"] == 0:
            ts = (vote_row["voted_at"] or "")[:16].replace("T", " ")
            vote_line = f"✅ <b>Yes</b> (voted at {ts} Almaty)"
        else:
            ts = (vote_row["voted_at"] or "")[:16].replace("T", " ")
            vote_line = f"❌ <b>No</b> (voted at {ts} Almaty)"

        if poll["message_id"]:
            link = make_poll_link(group_id, poll["message_id"])
            link_line = f'\n🔗 <a href="{link}">Go to today\'s poll</a>' if link else ""
        else:
            link_line = ""

    # Weekly stats so far
    week_start, week_end = get_current_week_bounds()
    weekly = await db.get_participant_stats_weekly(
        participant["id"], week_start.isoformat(), week_end.isoformat()
    )
    days_so_far = (get_almaty_today() - week_start).days + 1
    yes_count = weekly["total_yes"] if weekly else 0

    await msg.reply(
        f"📅 <b>Today ({today})</b>\n"
        f"• {vote_line}{link_line}\n\n"
        f"📈 This week: {yes_count}/{days_so_far} days ✅",
        parse_mode="HTML",
        disable_web_page_preview=True,
    )


# ---------------------------------------------------------------------------
# /stats
# ---------------------------------------------------------------------------

@router.message(Command("stats"), IsGroup())
async def cmd_stats(msg: Message, db: Database) -> None:
    group_id = msg.chat.id
    user_id = msg.from_user.id

    participant = await db.get_participant_by_user_id(group_id, user_id)
    if not participant or not participant["active"]:
        await msg.reply("❌ You're not a participant. Use /join to join the challenge.")
        return

    pid = participant["id"]
    week_start, week_end = get_current_week_bounds()
    weekly = await db.get_participant_stats_weekly(
        pid, week_start.isoformat(), week_end.isoformat()
    )
    alltime = await db.get_participant_stats_alltime(pid)

    def _rate(yes: int, total: int) -> str:
        return f"{yes / total * 100:.0f}%" if total > 0 else "—"

    w_yes = weekly["total_yes"] if weekly else 0
    w_no = weekly["total_no"] if weekly else 0
    w_missed = weekly["total_missed"] if weekly else 0
    w_total = w_yes + w_no + w_missed

    a_yes = alltime["total_yes"] if alltime else 0
    a_no = alltime["total_no"] if alltime else 0
    a_missed = alltime["total_missed"] if alltime else 0
    a_total = a_yes + a_no + a_missed

    name = format_mention(user_id, msg.from_user.username, msg.from_user.full_name)

    await msg.reply(
        f"📊 <b>Stats for {name}</b>\n\n"
        f"<b>This Week ({week_start.strftime('%b %d')}–{week_end.strftime('%b %d')}):</b>\n"
        f"  ✅ Yes:    {w_yes} days\n"
        f"  ❌ No:     {w_no} days\n"
        f"  😶 Missed: {w_missed} days\n"
        f"  📈 Rate:   {_rate(w_yes, w_total)}\n\n"
        f"<b>All Time:</b>\n"
        f"  ✅ Yes:    {a_yes} days\n"
        f"  ❌ No:     {a_no} days\n"
        f"  😶 Missed: {a_missed} days\n"
        f"  📈 Rate:   {_rate(a_yes, a_total)}",
        parse_mode="HTML",
    )


# ---------------------------------------------------------------------------
# /leaderboard
# ---------------------------------------------------------------------------

@router.message(Command("leaderboard"), IsGroup())
async def cmd_leaderboard(msg: Message, db: Database) -> None:
    group_id = msg.chat.id
    week_start, week_end = get_current_week_bounds()
    rows = await db.get_weekly_leaderboard(
        group_id, week_start.isoformat(), week_end.isoformat()
    )

    if not rows:
        await msg.reply(
            "No participants yet.\n"
            "Add participants with /add or use /join to self-enroll."
        )
        return

    days_so_far = (get_almaty_today() - week_start).days + 1
    medals = ["🥇", "🥈", "🥉"]

    lines = [
        f"🏆 <b>Leaderboard</b>\n"
        f"Week of {week_start.strftime('%b %d')} · {days_so_far}/7 days elapsed\n"
    ]

    for i, p in enumerate(rows):
        medal = medals[i] if i < 3 else f"{i + 1}."
        mention = format_mention(p["user_id"], p["username"], p["display_name"])
        yes = p["yes_count"]
        rate = f"{yes / days_so_far * 100:.0f}%" if days_so_far > 0 else "0%"
        fire = " 🔥" if yes == days_so_far else ""
        lines.append(f"{medal} {mention} — {yes}/{days_so_far} ({rate}){fire}")

    await msg.reply("\n".join(lines), parse_mode="HTML")
