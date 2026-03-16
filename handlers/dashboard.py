from datetime import datetime

class DashboardEngine:
    @staticmethod
    def format_currency(amount: float) -> str:
        return f"{amount:,.2 str} ETB"

    @staticmethod
    def get_greeting():
        hour = datetime.now().hour
        if hour < 12: return "Good Morning"
        if hour < 18: return "Good Afternoon"
        return "Good Evening"

    @staticmethod
    def calculate_payout_trend(current_spend, last_spend):
        """Returns a percentage increase/decrease for the UI"""
        if last_spend == 0: return 0
        return ((current_spend - last_spend) / last_spend) * 100