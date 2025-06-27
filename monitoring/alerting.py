import requests
import logging

# Place ici ton token et ton chat_id (ou charge-les depuis un fichier de config)
TELEGRAM_TOKEN = "7553601802:AAE2zqRIxe3fezxgkeyoCIp1mlQA6ysGE6Y"
TELEGRAM_CHAT_ID = "1063249882"

def send_telegram_alert(message, token=TELEGRAM_TOKEN, chat_id=TELEGRAM_CHAT_ID):
    """
    Envoie un message d'alerte sur Telegram.
    """
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML"
    }
    try:
        response = requests.post(url, data=payload, timeout=10)
        if response.status_code != 200:
            logging.error(f"[Telegram] Failed to send alert: {response.text}")
        else:
            logging.info(f"[Telegram] Alert sent: {message}")
    except Exception as e:
        logging.error(f"[Telegram] Exception: {e}")

# Exemple d'utilisation :
# send_telegram_alert("🚨 Ceci est un test d'alerte Telegram !")