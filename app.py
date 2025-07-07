from flask import Flask
from flask_socketio import SocketIO
import socket

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

def find_available_port(start_port=5000, end_port=5050):
    for port in range(start_port, end_port+1):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(('localhost', port))
                return port
            except OSError:
                continue
    raise OSError("No available ports found")

if __name__ == '__main__':
    port = find_available_port()
    print(f"Starting server on port {port}")
    socketio.run(app, port=port, debug=True)