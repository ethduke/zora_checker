import yaml
import os
import logging

CONFIG_FILE_PATH = "config.yaml"

def load_config():
    """Loads configuration from the YAML file."""
    if not os.path.exists(CONFIG_FILE_PATH):
        logging.error(f"Configuration file not found at {CONFIG_FILE_PATH}")
        return None
        
    try:
        with open(CONFIG_FILE_PATH, 'r') as f:
            config_data = yaml.safe_load(f)
        if not config_data:
             logging.error(f"Configuration file {CONFIG_FILE_PATH} is empty or invalid.")
             return None
        return config_data
    except yaml.YAMLError as e:
        logging.error(f"Error parsing configuration file {CONFIG_FILE_PATH}: {e}")
        return None
    except Exception as e:
        logging.exception(f"Error reading configuration file {CONFIG_FILE_PATH}: {e}")
        return None

_config = load_config()

if _config:
    API_URL = _config.get("api_url")
    PATHS = _config.get("paths", {})
    SETTINGS = _config.get("settings", {})

    ADDRESSES_FILE = PATHS.get("addresses")
    PROXIES_FILE = PATHS.get("proxies")
    QUERY_FILE = PATHS.get("query")
    OUTPUT_JSON_FILE = PATHS.get("output_json")

    DEFAULT_BATCH_SIZE = 10
    DEFAULT_BATCH_RATE_LIMIT_DELAY = 5
    DEFAULT_MAX_PERSISTENT_RETRIES = 50
    DEFAULT_MAX_REQUEST_TIMEOUT = 45

    BATCH_SIZE = SETTINGS.get("batch_size", DEFAULT_BATCH_SIZE)
    BATCH_RATE_LIMIT_DELAY = SETTINGS.get("batch_rate_limit_delay", DEFAULT_BATCH_RATE_LIMIT_DELAY)
    MAX_PERSISTENT_RETRIES_PER_ADDRESS = SETTINGS.get("max_persistent_retries_per_address", DEFAULT_MAX_PERSISTENT_RETRIES)
    MAX_REQUEST_TIMEOUT = SETTINGS.get("max_request_timeout", DEFAULT_MAX_REQUEST_TIMEOUT)

    _essential_paths = [ADDRESSES_FILE, PROXIES_FILE, QUERY_FILE, OUTPUT_JSON_FILE]
    _missing_paths = [name for name, value in zip(["paths.addresses", "paths.proxies", "paths.query", "paths.output_json"], _essential_paths) if value is None]

    if API_URL is None:
        _missing_paths.append("api_url")

    if _missing_paths:
        logging.error(f"Missing essential configuration keys in {CONFIG_FILE_PATH}: {', '.join(_missing_paths)}")
        exit(1)

    for name, value, default in [
        ("batch_size", BATCH_SIZE, DEFAULT_BATCH_SIZE),
        ("batch_rate_limit_delay", BATCH_RATE_LIMIT_DELAY, DEFAULT_BATCH_RATE_LIMIT_DELAY),
        ("max_persistent_retries_per_address", MAX_PERSISTENT_RETRIES_PER_ADDRESS, DEFAULT_MAX_PERSISTENT_RETRIES),
        ("max_request_timeout", MAX_REQUEST_TIMEOUT, DEFAULT_MAX_REQUEST_TIMEOUT)
    ]:
        if not isinstance(value, (int, float)) or value <= 0:
            logging.warning(f"Invalid or non-positive value for setting '{name}' ({value}). Using default: {default}")
            if name == "batch_size": BATCH_SIZE = default
            elif name == "batch_rate_limit_delay": BATCH_RATE_LIMIT_DELAY = default
            elif name == "max_persistent_retries_per_address": MAX_PERSISTENT_RETRIES_PER_ADDRESS = default
            elif name == "max_request_timeout": MAX_REQUEST_TIMEOUT = default

else:
    logging.error("Exiting due to configuration loading failure.")
    exit(1)


def read_file_lines(filepath):
    if not filepath:
        logging.error("Filepath not provided to read_file_lines.")
        return []
    try:
        with open(filepath, 'r') as f:
            return [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        logging.error(f"File not found at {filepath}")
        return []
    except Exception as e:
        logging.exception(f"Error reading file {filepath}: {e}")
        return []

def read_file_content(filepath):
    if not filepath:
        logging.error("Filepath not provided to read_file_content.")
        return None
    try:
        with open(filepath, 'r') as f:
            return f.read()
    except FileNotFoundError:
        logging.error(f"File not found at {filepath}")
        return None
    except Exception as e:
        logging.exception(f"Error reading file {filepath}: {e}")
        return None
