import yaml
import logging
import requests
from binance.um_futures import UMFutures

logger = logging.getLogger(__name__)

def init_binance_client(mode="testnet"):
    """
    Initialise le client Binance UMFutures pour testnet ou mainnet.
    
    Args:
        mode (str): "testnet" ou "live"

    Returns:
        UMFutures: client Binance UM Futures configure
    """
    try:
        with open("config/config.yaml", "r") as f:
            config = yaml.safe_load(f)
        
        key = config["binance"]["api_key"]
        secret = config["binance"]["api_secret"]

        if mode == "testnet":
            base_url = "https://testnet.binancefuture.com"
        elif mode == "live":
            base_url = "https://fapi.binance.com"
            test_response = requests.get(f"{base_url}/fapi/v1/time", headers={"X-MBX-APIKEY": key})
            if test_response.status_code == 401:
                logger.warning("[Binance Client] Warning: IP may not be whitelisted for live mode. Check API settings.")
        else:
            raise ValueError("Mode invalide : choisir 'testnet' ou 'live'")
        
        client = UMFutures(key=key, secret=secret, base_url=base_url)
        try:
            client.time()  # Test API connectivity
            logger.info("[Binance Client] API connectivity confirmed")
        except Exception as e:
            logger.error(f"[Binance Client] Connectivity test failed: {e}")
            return None
        
        logger.info(f"[Binance Client] Initialise en mode {mode} avec {base_url}")
        return client

    except Exception as e:
        logger.error(f"[Binance Client] Erreur d'initialisation : {e}")
        return None