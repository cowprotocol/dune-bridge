"""Modifies and executed dune query for today's data"""
from __future__ import annotations

import json
import os

from duneapi.api import DuneAPI
from duneapi.types import DuneQuery, Network


SOLVER_QUERY = """
select 
    concat('0x', encode(address, 'hex')) as address,
    environment,
    name, 
    active
from gnosis_protocol_v2.view_solvers
"""

SHORT_NAMES = {
    Network.MAINNET: "mainnet",
    Network.GCHAIN: "gc",
}


def store_solver_list(dune: DuneAPI, network: Network) -> list[dict[str, str]]:
    solver_list = dune.fetch(
        DuneQuery.from_environment(
            name="Solver List",
            description="",
            raw_sql=SOLVER_QUERY,
            network=network,
            parameters=[],
        )
    )
    filename = os.path.join(
        os.environ["DUNE_DATA_FOLDER"], f"{SHORT_NAMES[network]}-solvers.json"
    )
    with open(filename, "w+", encoding="utf-8") as f:
        json.dump(solver_list, f, ensure_ascii=False, indent=4)
    return solver_list


if __name__ == "__main__":
    dune_connection = DuneAPI.new_from_environment()
    for chain in [Network.MAINNET, Network.GCHAIN]:
        store_solver_list(dune=dune_connection, network=chain)
