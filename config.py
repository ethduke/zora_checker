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
    BATCH_SIZE = SETTINGS.get("batch_size", DEFAULT_BATCH_SIZE) 
    INITIAL_RETRY_DELAY = SETTINGS.get("initial_retry_delay", 5)
    MAX_PERSISTENT_RETRIES_PER_ADDRESS = SETTINGS.get("max_persistent_retries_per_address", 50)

    _essential_values = [API_URL, ADDRESSES_FILE, PROXIES_FILE, QUERY_FILE, OUTPUT_JSON_FILE]
    _missing_values = [name for name, value in zip(["API_URL", "ADDRESSES_FILE", "PROXIES_FILE", "QUERY_FILE", "OUTPUT_JSON_FILE"], _essential_values) if value is None]

    if _missing_values:
        logging.error(f"Missing essential configuration keys in {CONFIG_FILE_PATH}: {', '.join(_missing_values)}")
        exit(1)
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
