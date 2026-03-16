import os
from dataclasses import dataclass, field
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

def env_list(key: str) -> list[int]:
    val = os.getenv(key, "")
    return [int(x.strip()) for x in val.split(",") if x.strip()]

@dataclass(frozen=True)
class Settings:
    # Core Bot Settings
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
    ADMIN_IDS: list[int] = field(default_factory=lambda: env_list("ADMIN_IDS"))
    DATABASE_URL: str = os.getenv("DATABASE_URL", "")
    
    # Web App & Webhook
    WEBHOOK_BASE_URL: str = os.getenv("WEBHOOK_BASE_URL", "")
    PORT: int = int(os.getenv("PORT", "8080"))
    FRONTEND_ORIGIN: str = os.getenv("FRONTEND_ORIGIN", "http://127.0.0.1:5500")
    MINI_APP_URL: str = os.getenv("MINI_APP_URL", "")
    
    # Logging
    ADMIN_ERROR_LOG_ID: int = int(os.getenv("ADMIN_ERROR_LOG_ID", "0"))
    ADMIN_REPORT_LOG_ID: int = int(os.getenv("ADMIN_REPORT_LOG_ID", "0"))

settings = Settings()

# Ensure uploads directory exists
Path("./uploads").mkdir(parents=True, exist_ok=True)# Settings (Tokens, DSN, Admin IDs)
