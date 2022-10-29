"""
This module fetches the orderbook rewards from the prod and barn orderbook databases
and injects them into Dune as a user generated view. Note that the table is very large,
so results are paginated across tables of the form

dune_user_generated.cow_order_rewards_{{Environment}}_page_{{Page}}

a complete table is also built as the union of these under the name

dune_user_generated.cow_order_rewards_{{Environment}}

The intention is to run this script once every 24 hours.
We can optimize this data transfer by evaluating the checksum.
Essentially, it should only have to update the last page (or append a new page)
as long as the previous pages have not been tampered with.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Optional

from duneapi.api import DuneAPI
from pandas import DataFrame

from dune_api_scripts.local_env import DUNE_CONNECTION
from dune_api_scripts.pg_client import PgEngine
from dune_api_scripts.update.utils import Environment, update_args, multi_push_view
from dune_api_scripts.utils import hex2bytea

log = logging.getLogger(__name__)
log.level = log.iNFO

@dataclass
class OrderRewards:
    """OrderReward values"""

    solver: str
    tx_hash: str
    order_uid: str
    amount: float
    safe_liquidity: Optional[bool]

    @classmethod
    def from_dataframe(cls, pdf: DataFrame) -> list[OrderRewards]:
        """Constructs OrderReward records from Dataframe"""
        return [
            cls(
                solver=row["solver"],
                tx_hash=row["tx_hash"],
                order_uid=row["order_uid"],
                amount=float(row["amount"]),
                safe_liquidity=row["safe_liquidity"],
            )
            for _, row in pdf.iterrows()
        ]

    def __str__(self) -> str:
        solver, tx_hash, order_id = list(
            map(hex2bytea, [self.solver, self.tx_hash, self.order_uid])
        )
        safe = self.safe_liquidity if self.safe_liquidity is not None else "Null"
        return f"('{order_id}','{solver}','{tx_hash}',{self.amount},{safe})"


def fetch_and_push_order_rewards(dune: DuneAPI, env: Environment):
    """Fetches and parses Order Rewards from Orderbook, pushes them to Dune."""
    log.info("Fetching and Merging Orderbook Rewards")
    rewards = OrderRewards.from_dataframe(
        PgEngine().fetch_and_merge("orderbook/order_rewards.sql")
    )
    # TODO - In order or update less we can do a "checksum" of the pages being written
    #  and only update those pages which fail.
    #  Almost always, we should only have to update the last page.
    #  Our checksum should be the results of this SQL query:
    #  NOTE THAT: Checksums require rewards to be sorted!
    #   should be able to merge sort the two dataframes
    #   (but we will start by sorting the OrderRewards list it manually here)
    # with all_but_last_page as (
    #   either
    #   select union select * from union ... all but last page
    #   or
    #   select * from dune_user_generated.cow_order_rewards_{{Environment}}
    #   limit num_records / partition_size
    # ) The first option is more fault tolerant.
    #   We could also include page numbers in the rows (with some redundancy)
    # select
    #   solver,
    #   count(*) as num_trades,
    #   sum(case when safe_liquidity is True then 1 else 0 end) as num_safe,
    #   sum(case when safe_liquidity is False then 1 else 0 end) as num_unsafe,
    #   sum(amount) as raw_rewards
    # from all_but_last_page
    rewards.sort(key=lambda t: t.order_uid)

    log.info(f"Got {len(rewards)} records.")
    partition_size = 3000  # (~0.73Mb < 1Mb)
    values = list(map(str, rewards))

    log.info(f"Partitioning {len(values)} into chunks of size {partition_size}")
    multi_push_view(
        dune,
        query_file="user_generated_order_rewards.sql",
        aggregate_query_file="user_generated_aggregated_rewards.sql",
        base_table_name="cow_order_rewards",
        partitioned_values=[
            values[i: i + partition_size] for i in range(0, len(values), partition_size)
        ],
        env=env,
        query_id=os.environ.get("ORDER_REWARDS_QUERY", 1476356),
    )


if __name__ == "__main__":
    fetch_and_push_order_rewards(
        dune=DUNE_CONNECTION,
        env=update_args().environment,
    )
