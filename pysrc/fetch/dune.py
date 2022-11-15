import asyncio
import os
from dataclasses import dataclass

from dotenv import load_dotenv
from dune_client.types import QueryParameter, DuneRecord

from pysrc.dune_fetcher import DuneFetcher
from pysrc.dune_queries import QUERIES


@dataclass
class BlockRange:
    block_from: int
    block_to: int

    def __str__(self) -> str:
        return f"({self.block_from}, {self.block_to})"

    def as_query_params(self) -> list[QueryParameter]:
        return [
            QueryParameter.number_type("BlockFrom", self.block_from),
            QueryParameter.number_type("BlockTo", self.block_to),
        ]


async def app_hash_block_range(dune: DuneFetcher) -> BlockRange:
    return BlockRange(
        # TODO - could be replaced by Dune Query on the app_data table (once available).
        #  alternatively we can read this from the stored files... as encoded in filename or its own table.
        block_from=0,
        block_to=int(
            # TODO - this could result in key error
            (await dune.fetch(QUERIES["LATEST_APP_HASH_BLOCK"].query))[0][
                "latest_block"
            ]
        ),
    )


async def get_app_hashes(dune: DuneFetcher) -> tuple[BlockRange, list[DuneRecord]]:
    block_range = await app_hash_block_range(dune)
    app_hash_query = QUERIES["APP_HASHES"].with_params(block_range.as_query_params())
    app_hashes = await dune.fetch(app_hash_query)
    return block_range, app_hashes


async def main(dune: DuneFetcher) -> None:
    block_range, hash_data = await get_app_hashes(dune)
    # TODO - fetch AppContent from here before write.
    dune.write_results(hash_data, f"app_hashes_{block_range.block_to}.json")


if __name__ == "__main__":
    load_dotenv()
    asyncio.run(
        main(
            dune=DuneFetcher(os.environ["DUNE_API_KEY"]),
        )
    )
