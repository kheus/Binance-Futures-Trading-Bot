import requests
import yaml

with open("config/alerting_config.yaml", "r") as f:
    config = yaml.safe_load(f)

def send_telegram_alert(message):
    try:
        url = f"https://api.telegram.org/bot{config['telegram']['bot_token']}/sendMessage"
        payload = {
            "chat_id": config["telegram"]["chat_id"],
            "text": message
        }
        response = requests.post(url, json=payload)
        response.raise_for_status()
        print(f"Telegram alert sent: {message}")
    except Exception as e:
        print(f"Telegram alert error: {e}")
