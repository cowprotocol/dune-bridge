"""Main Entry point for app_hash sync"""
import asyncio
import os

from dotenv import load_dotenv

from pysrc.fetch.dune import DuneFetcher


async def main(dune: DuneFetcher) -> None:
    """App Hash Sync Logic"""
    # TODO - use runtime config values here:
    fname, column = "sync_block", "last_synced_block"
    block_range = await dune.app_hash_block_range(fname, column)

    hash_data = await dune.get_app_hashes(block_range)
    # TODO - fetch AppContent from here before write.

    # Write the most recent data and also record the block_from,
    # so that next run will know where to start
    dune.file_manager.write_ndjson(hash_data, f"app_hashes_{block_range.block_to}")
    # Write last sync block only after the data has been written.
    dune.file_manager.write_csv([{column: str(block_range.block_to)}], fname)


if __name__ == "__main__":
    load_dotenv()
    asyncio.run(
        main(
            dune=DuneFetcher(os.environ["DUNE_API_KEY"]),
        )
    )
