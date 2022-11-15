"""
All Dune Query executions should be routed through this file.
# TODO - this should (perhaps) belong to dune-client package including FileIO
"""
import asyncio
import logging
import sys

# TODO - import from one location: https://github.com/cowprotocol/dune-client/issues/29
from dune_client.client import DuneClient
from dune_client.query import Query
from dune_client.types import DuneRecord
from requests import HTTPError

from pysrc.file import FileIO


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
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s %(message)s")
        # TODO - use runtime parameter.
        self.logger.setLevel(logging.DEBUG)
        self.file_manager = FileIO()
        self.dune = DuneClient(api_key)

    def write_results(self, results: list[DuneRecord], filename: str) -> None:
        """
        Writes `results` to `filename` according to self.file_manager configuration
        """
        self.file_manager.write(results, filename)

    async def fetch(self, query: Query) -> list[DuneRecord]:
        """Async dune Fetcher with some exception handling."""
        self.logger.debug(f"Executing {query}")

        try:
            # TODO - https://github.com/cowprotocol/dune-client/issues/28
            response = await asyncio.to_thread(
                self.dune.refresh, query, ping_frequency=10
            )
            if response.state.is_complete():
                response_rows = response.get_rows()
                self.logger.debug(
                    f"Got {len(response_rows)} results for execution {response.execution_id}"
                )
                return response_rows

            message = (
                f"query execution {response.execution_id} incomplete {response.state}"
            )
            self.logger.error(message)
            raise RuntimeError(f"no results for {message}")
        except HTTPError as err:
            self.logger.error(f"Got {err} - Exiting")
            sys.exit()

    async def fetch_and_write(self, query: Query, filename: str) -> None:
        """Fetches dune data and writes raw results to file"""
        results = await self.fetch(query)
        self.write_results(results, filename)
