"""Main Entry point for app_hash sync"""
import json
import logging.config

from dune_client.file.interface import FileIO
from dune_client.types import DuneRecord

from pysrc.environment import OUT_DIR
from pysrc.fetch.dune import DuneFetcher
from pysrc.fetch.ipfs import Cid
from pysrc.models.block_range import BlockRange

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
                log.debug(
                    f"Still no content found for {app_hash} at CID {cid} after {attempts} attempts"
                )
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


async def sync_app_data(dune: DuneFetcher) -> None:
    """App Data Sync Logic"""

    file_manager = FileIO(OUT_DIR / "app_data")
    # TODO - use runtime config values here:
    sync_block_file, column, missing_fname, max_retries = (
        "sync_block.csv",
        "last_synced_block",
        "missing_app_hashes.json",
        3,
    )
    block_from = 12153262  # Genesis block
    try:
        block_from = int(file_manager.load_singleton(sync_block_file, "csv")[column])
    except FileNotFoundError:
        log.warning(
            f"block range file {sync_block_file} not found, using genesis block {block_from}"
        )
    except KeyError as err:
        message = (
            f"block range file {sync_block_file} does not contain column header {column}, "
            f"exiting to avoid duplication"
        )
        log.error(message)
        raise RuntimeError(message) from err
    block_range = BlockRange(
        # TODO - could be replaced by Dune Query on the app_data table (once available).
        #  https://github.com/cowprotocol/dune-bridge/issues/42
        block_from,
        block_to=await dune.latest_app_hash_block(),
    )
    dune_results = await dune.get_app_hashes(block_range)

    try:
        missing_records = file_manager.load_ndjson(missing_fname)
    except FileNotFoundError:
        missing_records = []

    data_handler = RecordHandler(new_rows=dune_results, missing_values=missing_records)
    found, not_found = data_handler.fetch_content_and_filter(max_retries)

    # Write the most recent data and also record the block_from,
    # so that next run will know where to start
    file_manager.write_ndjson(found, f"cow_{block_range.block_to}.json")
    # When not_found is empty, we want to overwrite the file (hence skip_empty=False)
    # This happens when all records in the file have attempts exceeding GIVE_UP_THRESHOLD
    file_manager.write_ndjson(not_found, missing_fname, skip_empty=False)
    # Write last sync block only after the data has been written.
    file_manager.write_csv([{column: str(block_range.block_to)}], sync_block_file)
