from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from config import settings
from db import Database

# 1. Initialize Bot with HTML parse mode for premium formatting
bot = Bot(
    token=settings.BOT_TOKEN, 
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)

# 2. Initialize Dispatcher (The main router)
dp = Dispatcher()

# 3. Initialize Database Singleton
# This uses the DSN from your .env file
db = Database(dsn=settings.DATABASE_URL)# Shared singletons (bot, dp, db)
