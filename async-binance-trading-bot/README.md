# Async Binance Trading Bot

This project implements an asynchronous trading bot using the Binance API. The bot is designed to trade cryptocurrencies based on predefined strategies, with proper error handling and graceful shutdown procedures.

## Project Structure

```
async-binance-trading-bot
├── src
│   ├── bot
│   │   ├── __init__.py
│   │   ├── main.py
│   │   ├── binance_client.py
│   │   ├── strategy.py
│   │   ├── order_manager.py
│   │   └── shutdown.py
│   ├── config
│   │   └── config.yaml
│   └── utils
│       ├── __init__.py
│       └── logger.py
├── requirements.txt
└── README.md
```

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/async-binance-trading-bot.git
   cd async-binance-trading-bot
   ```

2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

## Configuration

Before running the bot, you need to configure your API keys and trading parameters. Edit the `src/config/config.yaml` file to include your Binance API key and secret, as well as any other necessary settings.

## Usage

To start the trading bot, run the following command:
```
python src/bot/main.py
```

The bot will initialize the Binance client, start trading based on the defined strategies, and handle any errors gracefully.

## Graceful Shutdown

The bot includes a shutdown procedure that ensures all tasks are canceled and resources are released properly. You can stop the bot using `Ctrl+C`, and it will handle the shutdown process automatically.

## Contributing

Contributions are welcome! If you have suggestions for improvements or new features, feel free to open an issue or submit a pull request.

## License

This project is licensed under the MIT License. See the LICENSE file for more details.