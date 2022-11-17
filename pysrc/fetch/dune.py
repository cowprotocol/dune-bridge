"""
All Dune Query executions should be routed through this file.
TODO - Move reusable components into dune-client:
    https://github.com/cowprotocol/dune-bridge/issues/40
"""
import asyncio
import logging
import sys

from requests import HTTPError

from dune_client.client import DuneClient
from dune_client.file import FileIO
from dune_client.query import Query
from dune_client.types import DuneRecord

from pysrc.dune_queries import QUERIES
from pysrc.environment import OUT_DIR
from pysrc.models.block_range import BlockRange


class DuneFetcher:
    """
    Class containing, DuneClient, FileIO and a logger for convenient Dune Fetching.
    """

    def __init__(
        self,
        api_key: str,
    ) -> None:
        """
        Class constructor.
        Builds DuneClient from `api_key` along with a logger and FileIO object.
        """
        # It's a bit weird that the DuneClient also declares a log like this,
        # but it also doesn't make sense to inherit that log. Not sure what's best practise here.
        self.log = logging.getLogger(__name__)
        logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s %(message)s")
        # TODO - use runtime parameter. https://github.com/cowprotocol/dune-bridge/issues/41
        self.log.setLevel(logging.DEBUG)
        self.file_manager = FileIO(path=OUT_DIR)
        self.dune = DuneClient(api_key)

    async def fetch(self, query: Query) -> list[DuneRecord]:
        """Async Dune Fetcher with some exception handling."""
        self.log.debug(f"Executing {query}")

        try:
            # Tried to use the AsyncDuneClient, without success:
            # https://github.com/cowprotocol/dune-client/pull/31#issuecomment-1316045313
            response = await asyncio.to_thread(
                self.dune.refresh, query, ping_frequency=10
            )
            if response.state.is_complete():
                response_rows = response.get_rows()
                self.log.debug(
                    f"Got {len(response_rows)} results for execution {response.execution_id}"
                )
                return response_rows

            message = (
                f"query execution {response.execution_id} incomplete {response.state}"
            )
            self.log.error(message)
            raise RuntimeError(f"no results for {message}")
        except HTTPError as err:
            self.log.error(f"Got {err} - Exiting")
            sys.exit()

    async def app_hash_block_range(self, fname: str, column: str) -> BlockRange:
        """
        Block Range is used to app hash fetcher where to find the new records.
        block_from: read from file `fname` as a loaded singleton.
            - uses genesis block is no file exists (should only ever happen once)
            - raises RuntimeError if column specified does not exist.
        block_to: fetched from Dune as the last indexed block for "GPv2Settlement_call_settle"
        """
        # Genesis block:
        block_from = 12153262
        try:
            block_from = int(self.file_manager.load_singleton(fname, "csv")[column])
        except FileNotFoundError:
            self.log.warning(
                f"block range file {fname}.csv not found, using genesis block {block_from}"
            )
        except KeyError as err:
            message = (
                f"block range file {fname}.csv does not contain column header {column}, "
                f"exiting to avoid duplication"
            )
            self.log.error(message)
            raise RuntimeError(message) from err

        return BlockRange(
            # TODO - could be replaced by Dune Query on the app_data table (once available).
            #  https://github.com/cowprotocol/dune-bridge/issues/42
            block_from,
            block_to=int(
                # KeyError here means the query has been modified and column no longer exists
                # IndexError means no results were returned from query (which is unlikely).
                (await self.fetch(QUERIES["LATEST_APP_HASH_BLOCK"].query))[0][
                    "latest_block"
                ]
            ),
        )

    async def get_app_hashes(self, block_range: BlockRange) -> list[DuneRecord]:
        """
        Executes APP_HASHES query for the given `block_range` and returns the results
        """
        app_hash_query = QUERIES["APP_HASHES"].with_params(
            block_range.as_query_params()
        )
        return await self.fetch(app_hash_query)
