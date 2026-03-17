"""
Discord bot entry point.
Loads environment variables from .env and starts the bot.
"""
 
from dotenv import load_dotenv
load_dotenv()
 
from tle.__main__ import main
 
if __name__ == '__main__':
    main()
 