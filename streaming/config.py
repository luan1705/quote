import os
from pathlib import Path

from dotenv import load_dotenv
from redis import Redis
from ssi_sdk import Config

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

REDIS_URL = os.getenv("REDIS_URL")
CHANNEL_PREFIX = os.getenv("CHANNEL_PREFIX")

CHANNEL_ASSET = f"{CHANNEL_PREFIX}asset"

REDIS = Redis.from_url(REDIS_URL, decode_responses=True)

SSI_CONFIG = Config(
    client_id=os.getenv("CLIENT_ID"),
    api_key=os.getenv("API_KEY"),
    api_secret=os.getenv("API_SECRET"),
    private_key=os.getenv("PRIVATE_KEY"),
)
