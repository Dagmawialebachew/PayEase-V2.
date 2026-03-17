from aiogram import Router, F
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from aiogram.filters import CommandStart, Command
from config import settings

router = Router()

@router.message(CommandStart())
async def cmd_start(message: Message):
    """Welcomes the admin and provides the Mini App entry point."""
    # Security Check: Only allow configured Admin IDs
    if message.from_user.id not in settings.ADMIN_IDS:
        return await message.answer("⚠️ <b>Access Denied.</b>\nThis system is restricted to authorized personnel.")

    # Premium Launch Button
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="🚀 Open Payease", 
                web_app=WebAppInfo(url=settings.MINI_APP_URL)
            )
        ]
    ])

    await message.answer(
        f"👋 <b>Welcome back, {message.from_user.first_name}!</b>\n\n"
        "The Wage & Loan Management system is ready.\n"
        "Tap the button below to manage workers, loans, and club payouts.",
        reply_markup=markup
    )

@router.message(Command("help"))
async def cmd_help(message: Message):
    """Basic guidance for the bot interface."""
    help_text = (
        "🛠 <b>Quick Guide</b>\n\n"
        "1. Click 'Open Payease' to access the dashboard.\n"
        "2. Toggle workers 'Active' to include them in today's wages.\n"
        "3. Record loans in the 'Loans' tab; they auto-deduct on payout.\n\n"
        "<i>For technical support, contact the system developer.</i>"
    )
    await message.answer(help_text)

# Export all_routers for bot.py to pick up
all_routers = [router]