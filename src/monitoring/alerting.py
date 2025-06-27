
import requests
import yaml
import logging
from decouple import config
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

with open("config/alerting_config.yaml", "r", encoding="utf-8-sig") as f:
    alerting_config = yaml.safe_load(f)

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
def send_telegram_alert(message):
    try:
        if not alerting_config["telegram"]["enabled"]:
            return
        url = f"https://api.telegram.org/bot{config('TELEGRAM_BOT_TOKEN')}/sendMessage"
        payload = {
            "chat_id": config("TELEGRAM_CHAT_ID"),
            "text": message
        }
        response = requests.post(url, json=payload)
        response.raise_for_status()
        logger.info(f"Telegram alert sent: {message}")
    except Exception as e:
        logger.error(f"Telegram alert error: {e}")
