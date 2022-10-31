"""
A single location where all common environment variables are parsed.
Each script will likely import something from here.
"""
import os
from pathlib import Path

from dotenv import load_dotenv
from duneapi.api import DuneAPI

load_dotenv()

# TODO - avoid redundant use of "data" in "dune_data"
DUNE_DATA_DIR = os.environ.get("DUNE_DATA_FOLDER", "./data/dune_data")

# TODO - rename `dune_api_scripts` to anything shorter (e.g. scripts or tasks)
QUERY_DIR = os.environ.get("QUERY_DIR", "./dune_api_scripts/queries")

# TODO - every script should just import this!
DUNE_CONNECTION = DuneAPI.new_from_environment()

# TODO - every relative query path should use these.
PROJECT_ROOT = Path(__file__).parent
QUERY_ROOT = PROJECT_ROOT / Path("queries/")
LOG_CONFIG_FILE = PROJECT_ROOT / Path("logging.conf")
