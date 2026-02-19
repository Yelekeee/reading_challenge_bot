"""
Handles PollAnswer updates — the core of individual vote tracking.

Telegram delivers PollAnswer updates only for non-anonymous polls created by
the bot. Each update contains the voter's user_id and their chosen option_ids.

Key behaviours:
  • option_ids=[0]  → voted ✅ Yes
  • option_ids=[1]  → voted ❌ No
  • option_ids=[]   → vote retracted (treated as 'missed' at snapshot time)
  • Vote changes are handled by UPSERTing the votes row.
  • If the voter is a pending participant (added by @username), their user_id
    is resolved here as a bonus side-effect.
"""

import logging

from aiogram import Router
from aiogram.types import PollAnswer

from db import Database

logger = logging.getLogger(__name__)
router = Router()


@router.poll_answer()
async def on_poll_answer(poll_answer: PollAnswer, db: Database) -> None:
    voter_id = poll_answer.user.id
    tg_poll_id = poll_answer.poll_id

    # Correlate to our DB poll record
    poll = await db.get_poll_by_tg_id(tg_poll_id)
    if not poll:
        # Not our poll (e.g. manually created by an admin) — ignore silently
        return

    # Decode option: empty list means retraction
    if poll_answer.option_ids:
        option_idx: int | None = poll_answer.option_ids[0]
    else:
        option_idx = None

    await db.upsert_vote(poll["id"], voter_id, option_idx)

    vote_label = {0: "✅ Yes", 1: "❌ No", None: "retracted"}[option_idx]
    logger.info(
        "Vote recorded: user=%s poll_id=%s group=%s option=%s",
        voter_id, poll["id"], poll["group_id"], vote_label,
    )

    # Resolve pending participant if this voter matches a queued @username
    user = poll_answer.user
    if user.username:
        resolved = await db.resolve_pending_by_username(
            poll["group_id"],
            user.username,
            voter_id,
            user.full_name,
        )
        if resolved:
            logger.info(
                "Resolved pending participant @%s via poll vote (group=%s)",
                user.username, poll["group_id"],
            )
