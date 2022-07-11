"""Modifies and executed dune query for today's data"""
from __future__ import annotations

import dataclasses
import json
import os
from dataclasses import dataclass
from typing import Any

from duneapi.api import DuneAPI
from duneapi.types import DuneQuery, Network
from duneapi.types import Address


class EnhancedJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        return super().default(o)


@dataclass
class Solver:
    address: str
    environment: str
    name: str
    active: bool

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Solver:
        return cls(
            address=Address(data["address"]).address,
            environment=data["environment"],
            name=data["name"],
            active=data["active"],
        )


def store_solver_list(dune: DuneAPI) -> list[Solver]:
    # TODO - fetch for both networks and merge JSON content with chainID
    # Waiting on https://github.com/duneanalytics/abstractions/pull/1268
    raw_solver_list = dune.fetch(
        DuneQuery.from_environment(
            name="Solver List",
            description="",
            raw_sql="select * from gnosis_protocol_v2.view_solvers",
            network=Network.MAINNET,
            parameters=[],
        )
    )
    solver_list = [Solver.from_dict(rec) for rec in raw_solver_list]

    filename = os.path.join(os.environ["DUNE_DATA_FOLDER"], "solvers.json")
    with open(filename, "w+", encoding="utf-8") as f:
        json.dump(solver_list, f, ensure_ascii=False, indent=4, cls=EnhancedJSONEncoder)

    return solver_list


if __name__ == "__main__":

    store_solver_list(dune=DuneAPI.new_from_environment())
