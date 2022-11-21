"""Main Entry point for app_hash sync"""
import asyncio
import logging.config
import os

from dotenv import load_dotenv

from pysrc.sync import sync_app_data
from pysrc.fetch.dune import DuneFetcher

log = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s %(message)s")
log.setLevel(logging.DEBUG)


GIVE_UP_THRESHOLD = 10


if __name__ == "__main__":
    load_dotenv()
    asyncio.run(
        sync_app_data(
            dune=DuneFetcher(os.environ["DUNE_API_KEY"]),
        )
    )
