"""
Discord bot entry point.
Loads environment variables from .env and starts the bot.
"""
# ruff: noqa: E402
from dotenv import load_dotenv

load_dotenv()

from tle.__main__ import main  # noqa: E402

if __name__ == '__main__':
    main()
