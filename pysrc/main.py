"""Main Entry point for app_hash sync"""
import asyncio
import logging.config
import os

from dotenv import load_dotenv
from dune_client.types import DuneRecord

from pysrc.fetch.dune import DuneFetcher
from pysrc.fetch.ipfs import Cid

log = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s %(message)s")
log.setLevel(logging.DEBUG)


def fetch_and_filter(
    dune_results: list[DuneRecord],
    max_retries: int = 3,
) -> tuple[list[DuneRecord], list[DuneRecord]]:
    """
    Run loop fetching app_data for hashes,
    separates into (found and not found), returning the pair.
    """
    found: list[dict[str, str]] = []
    not_found: list[dict[str, str]] = []
    # Drain the dune_results into "found" and "not found" categories
    while dune_results:
        row = dune_results.pop()
        app_hash = row["app_hash"]
        cid = Cid(app_hash)
        app_data = cid.get_content(max_retries)

        # Here it would be nice if python we more like rust!
        if app_data is not None:
            # Row is modified and added found items
            log.debug(f"Found content for {row['app_hash']} at CID {cid}")
            row["content"] = app_data
            found.append(row)
        else:
            # Unmodified row added to not_found items
            log.debug(
                f"No content found for {row['app_hash']} at CID {cid} after {max_retries} retries"
            )
            not_found.append(row)

    return found, not_found


async def main(dune: DuneFetcher) -> None:
    """App Hash Sync Logic"""
    # TODO - use runtime config values here:
    fname, column = "sync_block", "last_synced_block"
    block_range = await dune.app_hash_block_range(fname, column)

    dune_results = await dune.get_app_hashes(block_range)

    # TODO - although it is unlikely we will ever "cold-start",
    #  it seems that storing all this data in memory could be problematic
    #  e.g. program fails and all progress is lost, etc...
    #  It might be nice to append the results directly to the stream as they are found.
    #  I am going to implement append anyway (for the missing records)
    found, not_found = fetch_and_filter(dune_results)

    # Write the most recent data and also record the block_from,
    # so that next run will know where to start
    dune.file_manager.write_ndjson(found, f"app_hashes_{block_range.block_to}")
    # TODO - "append" missing to an existing file.
    dune.file_manager.write_ndjson(
        not_found, f"missing_app_hashes_{block_range.block_to}"
    )
    # Write last sync block only after the data has been written.
    dune.file_manager.write_csv([{column: str(block_range.block_to)}], fname)


if __name__ == "__main__":
    load_dotenv()
    asyncio.run(
        main(
            dune=DuneFetcher(os.environ["DUNE_API_KEY"]),
        )
    )
