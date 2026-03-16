import asyncio
import logging
import os
import sys

from aiohttp import web
import aiohttp_cors

from aiogram import Bot, Dispatcher
from aiogram.types import BotCommand, BotCommandScopeDefault, BotCommandScopeChat
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

from config import settings
from app_context import bot, dp, db
# from middlewares.language import LanguageMiddleware
# from middlewares.throttling_middleware import ThrottlingMiddleware
# from middlewares.error_handling_middleware import router as error_router

# API and Handlers
from api.api import setup_admin_routes
from api.api import initialize_report_engine
# from scheduler.scheduler import check_and_send_reminders

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

# # --- Middleware Registration ---
# dp.message.middleware(ThrottlingMiddleware(message_interval=0.6))
# dp.callback_query.middleware(ThrottlingMiddleware(message_interval=0.4))

# # Language middleware for multilingual support (English/Amharic)
# dp.message.middleware(LanguageMiddleware(db))
# dp.callback_query.middleware(LanguageMiddleware(db))

# --- Router Registration ---
from handlers import all_routers
for r in all_routers:
    dp.include_router(r)

# dp.include_router(error_router) # Error handling last

# --- Bot Commands ---
async def set_commands(bot: Bot, admin_ids: list[int]):
    admin_commands = [
        BotCommand(command="start", description="🚀 Open Payease"),
    ]

    try:
        # Global default (for anyone else who finds the bot)
        await bot.set_my_commands([BotCommand(command="start", description="🚀 Welcome")], scope=BotCommandScopeDefault())
        
        # Specific commands for your uncle (the Admin)
        for admin_id in admin_ids:
            await bot.set_my_commands(admin_commands, scope=BotCommandScopeChat(chat_id=admin_id))
    except Exception as e:
        logging.exception("Failed to set bot commands: %s", e)

# --- Lifecycle Hooks ---
async def on_startup(bot: Bot):
    logging.info("🚀 Initializing Payease...")
    await db.connect()
    await db.setup()  # Initializes tables
    await set_commands(bot, settings.ADMIN_IDS)

    if settings.WEBHOOK_BASE_URL:
        webhook_url = f"{settings.WEBHOOK_BASE_URL}/webhook"
        await bot.set_webhook(webhook_url, drop_pending_updates=True)
        logging.info(f"Webhook set to: {webhook_url}")

# async def scheduler_loop(bot: Bot, db):
#     """Background task for automated reminders/payout flags."""
#     while True:
#         try:
#             await check_and_send_reminders(bot, db)
#         except Exception as e:
#             logging.error(f"Scheduler Error: {e}")
        
#         # Check every 6 hours
#         await asyncio.sleep(21600)

async def on_shutdown(bot: Bot):
    logging.info("🛑 Shutting down engine...")
    await db.disconnect()
    await bot.session.close()

# --- App Factory ---
async def create_app() -> web.Application:
    app = web.Application()
    app["bot"] = bot
    app["db"] = db

    # Health Check
    app.router.add_get("/health", lambda _: web.json_response({"status": "active"}))

    # Register Webhook
    webhook_handler = SimpleRequestHandler(dispatcher=dp, bot=bot)
    webhook_handler.register(app, path="/webhook")

    # Setup API Routes (The backend for your Mini App)
    initialize_report_engine(db, bot)
    setup_admin_routes(app)

    # Static Files (For reports and exported files)
    app.router.add_static("/uploads", "./uploads", show_index=False)

    # CORS Setup for the Frontend
    cors = aiohttp_cors.setup(app, defaults={
        settings.FRONTEND_ORIGIN: aiohttp_cors.ResourceOptions(
            allow_credentials=True, expose_headers="*", allow_headers="*", allow_methods="*"
        ),
        "*": aiohttp_cors.ResourceOptions(allow_headers="*", allow_methods="*")
    })

    for route in list(app.router.routes()):
        if not isinstance(route.resource, web.StaticResource):
            cors.add(route)

    setup_application(app, dp, bot=bot)

    async def startup_wrapper(_):
        await on_startup(bot)
        # asyncio.create_task(scheduler_loop(bot, db))
        await start_scheduler(db)

    app.on_startup.append(startup_wrapper)
    app.on_cleanup.append(lambda _: asyncio.create_task(on_shutdown(bot)))
    
    return app


from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime

# Initialize the scheduler
scheduler = AsyncIOScheduler()
# async def start_scheduler(db):
#     # Avoid double-starting if the app reloads
#     if not scheduler.running:
#         # Schedule the heartbeat to run daily at 11:59 PM
#         scheduler.add_job(
#             db.record_daily_attendance, 
#             'cron', 
#             hour=23, 
#             minute=59,
#             id="daily_attendance_job",
#             replace_existing=True
#         )
#         scheduler.start()
#         logging.info("📅 Payease Scheduler: Tracking active workers daily at 23:59")
        
        
async def start_scheduler(db):
    if not scheduler.running:
        # Changed 'cron' to 'interval' for rapid testing
        scheduler.add_job(
            db.record_daily_attendance, 
            'interval', 
            hours=5,
            id="daily_attendance_job",
            replace_existing=True
        )
        scheduler.start()
        logging.info("⚡️ Payease Test Mode: Recording attendance every 5 hours seconds")
        
        
# --- Execution ---
if __name__ == "__main__":
    if "--polling" in sys.argv:
        async def main_polling():
            await on_startup(bot)
            await bot.delete_webhook(drop_pending_updates=True)
            await start_scheduler(db)
            # asyncio.create_task(scheduler_loop(bot, db))
            try:
                await dp.start_polling(bot)
            finally:
                await on_shutdown(bot)
        asyncio.run(main_polling())
    else:
        web.run_app(create_app(), host="0.0.0.0", port=settings.PORT)# Entrypoint for aiogram bot
