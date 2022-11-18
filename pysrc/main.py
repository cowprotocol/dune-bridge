"""Main Entry point for app_hash sync"""
import asyncio
import json
import logging.config
import os

from dotenv import load_dotenv
from dune_client.types import DuneRecord

from pysrc.fetch.dune import DuneFetcher
from pysrc.fetch.ipfs import Cid

log = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s %(message)s")
log.setLevel(logging.DEBUG)


GIVE_UP_THRESHOLD = 10


class RecordHandler:  # pylint:disable=too-few-public-methods
    """
    This class is responsible for consuming new dune records and missing values from previous runs
    it attempts to fetch content for them and filters them into "found" and "not found" as necessary
    """

    def __init__(self, new_rows: list[DuneRecord], missing_values: list[DuneRecord]):
        self.found: list[dict[str, str]] = []
        self.not_found: list[dict[str, str]] = []

        self.new_rows = new_rows
        self.missing_values = missing_values

    def _handle_new_records(self, max_retries: int) -> None:
        # Drain the dune_results into "found" and "not found" categories
        while self.new_rows:
            row = self.new_rows.pop()
            app_hash = row["app_hash"]
            cid = Cid(app_hash)
            app_data = cid.get_content(max_retries)

            # Here it would be nice if python we more like rust!
            if app_data is not None:
                # Row is modified and added found items
                log.debug(f"Found content for {app_hash} at CID {cid}")
                row["content"] = app_data
                self.found.append(row)
            else:
                # Unmodified row added to not_found items
                log.debug(
                    f"No content found for {app_hash} at CID {cid} after {max_retries} retries"
                )
                # Dune Records are string dicts.... :(
                row["attempts"] = str(max_retries)
                self.not_found.append(row)

    def _handle_missing_records(self, max_retries: int) -> None:
        while self.missing_values:
            row = self.missing_values.pop()
            app_hash = row["app_hash"]
            cid = Cid(app_hash)
            app_data = cid.get_content(max_retries)
            attempts = int(row["attempts"]) + max_retries

            if app_data is not None:
                log.debug(
                    f"Found previously missing content hash {row['app_hash']} at CID {cid}"
                )
                self.found.append(
                    {
                        "app_hash": app_hash,
                        "first_seen_block": row["first_seen_block"],
                        "content": app_data,
                    }
                )
            elif app_data is None and attempts > GIVE_UP_THRESHOLD:
                log.debug(
                    f"No content found after {attempts} attempts for {app_hash} assuming NULL."
                )
                self.found.append(
                    {
                        "app_hash": app_hash,
                        "first_seen_block": row["first_seen_block"],
                        "content": json.dumps({}),
                    }
                )
            else:
                row.update({"attempts": str(attempts)})
                self.not_found.append(row)

    def fetch_content_and_filter(
        self, max_retries: int
    ) -> tuple[list[DuneRecord], list[DuneRecord]]:
        """
        Run loop fetching app_data for hashes,
        separates into (found and not found), returning the pair.
        """
        self._handle_new_records(max_retries)
        log.info(
            f"Attempting to recover missing {len(self.missing_values)} records from previous run"
        )
        self._handle_missing_records(max_retries)
        return self.found, self.not_found


async def main(dune: DuneFetcher) -> None:
    """App Hash Sync Logic"""
    # TODO - use runtime config values here:
    fname, column, missing_fname, max_retries = (
        "sync_block",
        "last_synced_block",
        "missing_app_hashes",
        3,
    )
    block_range = await dune.app_hash_block_range(fname, column)
    dune_results = await dune.get_app_hashes(block_range)

    try:
        missing_records = dune.file_manager.load_ndjson(missing_fname)
    except FileNotFoundError:
        missing_records = []

    data_handler = RecordHandler(new_rows=dune_results, missing_values=missing_records)
    found, not_found = data_handler.fetch_content_and_filter(max_retries)

    # Write the most recent data and also record the block_from,
    # so that next run will know where to start
    dune.file_manager.write_ndjson(found, f"app_content_{block_range.block_to}")
    dune.file_manager.write_ndjson(not_found, missing_fname)
    # Write last sync block only after the data has been written.
    dune.file_manager.write_csv([{column: str(block_range.block_to)}], fname)


if __name__ == "__main__":
    load_dotenv()
    asyncio.run(
        main(
            dune=DuneFetcher(os.environ["DUNE_API_KEY"]),
        )
    )
