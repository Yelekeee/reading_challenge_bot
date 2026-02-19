import os
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN: str = os.environ["BOT_TOKEN"]
DATABASE_PATH: str = os.getenv("DATABASE_PATH", "challenge.db")
DEFAULT_POLL_TIME: str = "20:00"
TIMEZONE: str = "Asia/Almaty"
