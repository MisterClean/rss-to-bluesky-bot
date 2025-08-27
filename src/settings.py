import os
import logging
import yaml
from dotenv import load_dotenv
from datetime import datetime

logger = logging.getLogger(__name__)

load_dotenv()

BLUESKY_USERNAME = os.getenv("BLUESKY_USERNAME")
BLUESKY_PASSWORD = os.getenv("BLUESKY_PASSWORD")
RSS_FEED_URL = os.getenv("RSS_FEED_URL")

# Minimum date for RSS entries to be posted (default to November 13, 2024)
MIN_POST_DATE = os.getenv("MIN_POST_DATE", "2024-11-13")
MIN_POST_DATE = datetime.strptime(MIN_POST_DATE, "%Y-%m-%d")

def load_config():
    """Load configuration from YAML file."""
    try:
        config_path = os.path.join(os.path.dirname(__file__), '../config.yaml')
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.critical(f"Failed to load config file: {e}")
        raise

config = load_config()
