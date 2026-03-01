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
    get_current_month_bounds,
    get_current_week_bounds,
    make_poll_link,
)

from config import DEFAULT_POLL_TIME

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


# ---------------------------------------------------------------------------
# /help
# ---------------------------------------------------------------------------

@router.message(Command("help"), IsGroup())
async def cmd_help(msg: Message, db: Database) -> None:
    settings = await db.get_settings(msg.chat.id)
    poll_time = settings["poll_time"] if settings else DEFAULT_POLL_TIME
    reminder_time = settings["reminder_time"] if settings else "22:00"
    status = "✅ Белсенді" if settings and settings["challenge_active"] else "⏸ Тоқтатылған"

    await msg.reply(
        f"📚 <b>Күнделікті оқу челленджі</b>\n\n"
        f"Күн сайын 30 минут оқып, дағдыны қалыптастырыңыз!\n\n"
        f"📊 Статус: {status}\n"
        f"⏰ Сауалнама: <b>{poll_time}</b>\n"
        f"⚠️ Еске салу: <b>{reminder_time}</b>\n\n"
        f"<b>Командалар:</b>\n"
        f"/join — челленджге қосылу\n"
        f"/leave — челленджден шығу\n"
        f"/today — бүгінгі дауыс беру статусы\n"
        f"/stats — апталық және жалпы статистика\n"
        f"/leaderboard — ағымдағы апта кестесі\n"
        f"/monthly — айлық кесте\n"
        f"/help — осы мәзір\n\n"
        f"<b>Админ командалары:</b>\n"
        f"/challenge_start — челленджді бастау\n"
        f"/challenge_stop — челленджді тоқтату\n"
        f"/set_time HH:MM — сауалнама уақытын өзгерту\n"
        f"/set_reminder_time HH:MM — еске салу уақытын өзгерту\n"
        f"/add — мүше қосу (жауап немесе @username)\n"
        f"/addall @n1 @n2 ... — бірнеше мүшені бірден қосу\n"
        f"/remove — мүшені жою\n"
        f"/participants — мүшелер тізімі\n"
        f"/weekly_summary_now — апталық қорытынды",
        parse_mode="HTML",
    )


# ---------------------------------------------------------------------------
# /monthly
# ---------------------------------------------------------------------------

@router.message(Command("monthly"), IsGroup())
async def cmd_monthly(msg: Message, db: Database) -> None:
    group_id = msg.chat.id
    month_start, month_end = get_current_month_bounds()
    today = get_almaty_today()
    days_so_far = (today - month_start).days + 1

    rows = await db.get_monthly_leaderboard(
        group_id, month_start.isoformat(), month_end.isoformat()
    )
    if not rows:
        await msg.reply(
            "No participants yet.\n"
            "Add participants with /add or use /join to self-enroll."
        )
        return

    medals = ["🥇", "🥈", "🥉"]
    lines = [
        f"📅 <b>{month_start.strftime('%B %Y')}</b> — Reading Challenge\n"
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

    await msg.reply("\n".join(lines), parse_mode="HTML")
