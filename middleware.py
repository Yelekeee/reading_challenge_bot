"""
Middleware and filters for the reading-challenge bot.

ParticipantResolverMiddleware  — passively resolves pending participants
                                 whenever they send any message in the group.
IsGroup                        — filter: only allow group/supergroup messages.
IsAdmin                        — filter: only allow chat administrators/creators.
"""

import logging
from typing import Any, Awaitable, Callable, Dict

from aiogram import BaseMiddleware, Bot
from aiogram.filters import BaseFilter
from aiogram.types import Message

from db import Database

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Filters
# ---------------------------------------------------------------------------

class IsGroup(BaseFilter):
    """Passes only when the message originates from a group or supergroup."""

    async def __call__(self, message: Message) -> bool:
        return message.chat.type in ("group", "supergroup")


class IsAdmin(BaseFilter):
    """Passes only when the sender is a chat administrator or creator."""

    async def __call__(self, message: Message, bot: Bot) -> bool:
        if message.chat.type not in ("group", "supergroup"):
            return False
        try:
            member = await bot.get_chat_member(message.chat.id, message.from_user.id)
            return member.status in ("administrator", "creator")
        except Exception:
            return False


# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------

class GroupRegistrationMiddleware(BaseMiddleware):
    """
    Ensures every group the bot receives messages from is registered in the DB.
    Also resolves pending participants whose username we now see.
    """

    async def __call__(
        self,
        handler: Callable[[Message, Dict[str, Any]], Awaitable[Any]],
        event: Message,
        data: Dict[str, Any],
    ) -> Any:
        db: Database = data.get("db")
        if db and event.chat and event.chat.type in ("group", "supergroup"):
            # Register the group (idempotent)
            await db.get_or_create_group(event.chat.id, event.chat.title or "")

            # Resolve any pending participant whose username we now know
            user = event.from_user
            if user and user.username:
                resolved = await db.resolve_pending_by_username(
                    event.chat.id,
                    user.username,
                    user.id,
                    user.full_name,
                )
                if resolved:
                    logger.info(
                        "Resolved pending participant @%s (user_id=%s) in group %s",
                        user.username, user.id, event.chat.id,
                    )

        return await handler(event, data)
