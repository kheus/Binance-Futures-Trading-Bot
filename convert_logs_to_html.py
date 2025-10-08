# convert_logs_to_html.py
import json
import pandas as pd

def convert_to_html(log_file: str, output_html: str):
    logs = []
    with open(log_file, 'r') as f:
        for line in f:
            try:
                logs.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    
    with open(output_html, 'w', encoding='utf-8') as f:
        f.write("<html><body><h1>Trading Bot Logs</h1><table border='1'>")
        f.write("<tr><th>Time</th><th>Level</th><th>Message</th><th>Extra</th></tr>")
        for log in logs:
            time = log.get('time', '')
            level = log.get('level', '')
            message = log.get('message', '')
            extra = log.get('extra', {})
            f.write(f"<tr><td>{time}</td><td>{level}</td><td>{message}</td><td>{extra}</td></tr>")
        f.write("</table></body></html>")

if __name__ == "__main__":
    convert_to_html("logs/main_bot.json", "logs/main_bot.html")
    convert_to_html("logs/signal_generator.json", "logs/signal_generator.html")# convert_logs_to_html.py
import json
import pandas as pd

def convert_to_html(log_file: str, output_html: str):
    logs = []
    with open(log_file, 'r') as f:
        for line in f:
            try:
                logs.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    
    with open(output_html, 'w', encoding='utf-8') as f:
        f.write("<html><body><h1>Trading Bot Logs</h1><table border='1'>")
        f.write("<tr><th>Time</th><th>Level</th><th>Message</th><th>Extra</th></tr>")
        for log in logs:
            time = log.get('time', '')
            level = log.get('level', '')
            message = log.get('message', '')
            extra = log.get('extra', {})
            f.write(f"<tr><td>{time}</td><td>{level}</td><td>{message}</td><td>{extra}</td></tr>")
        f.write("</table></body></html>")

if __name__ == "__main__":
    convert_to_html("logs/main_bot.json", "logs/main_bot.html")
    convert_to_html("logs/signal_generator.json", "logs/signal_generator.html")