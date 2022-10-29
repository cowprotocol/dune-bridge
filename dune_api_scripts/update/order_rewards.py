from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

from duneapi.api import DuneAPI
from pandas import DataFrame

from dune_api_scripts.local_env import DUNE_CONNECTION
from dune_api_scripts.pg_client import PgEngine
from dune_api_scripts.update.utils import Environment, push_view, update_args
from dune_api_scripts.utils import hex2bytea


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
        pdf = pdf.astype({"safe_liquidity": "boolean"})
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
        safe = self.safe_liquidity
        safe = safe if safe is not None else "Null"
        return f"('{solver}','{tx_hash}','{order_id}',{self.amount},{safe})"


def fetch_and_push_order_rewards(dune: DuneAPI, env: Environment):
    print("Fetching and Merging Orderbook Rewards")
    rewards = OrderRewards.from_dataframe(
        PgEngine().fetch_and_merge("orderbook/order_rewards.sql")
    )
    print(f"Got {len(rewards)} records.")
    push_view(
        dune,
        query_file="user_generated_order_rewards.sql",
        values=list(map(str, rewards)),  # Works as slice of size 1/N, with N>=20
        env=env,
        query_id=os.environ.get("ORDER_REWARDS_QUERY", 1476356)
    )


if __name__ == "__main__":
    fetch_and_push_order_rewards(
        dune=DUNE_CONNECTION,
        env=update_args().environment,
    )
