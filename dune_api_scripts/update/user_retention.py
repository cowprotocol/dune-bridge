"""Fetch Retention Data on Given Date from DuneAnalytics"""
from __future__ import annotations

import csv
import os
from dataclasses import dataclass, astuple

from datetime import datetime, date, timedelta

from duneapi.api import DuneAPI
from duneapi.types import Network, QueryParameter, DuneQuery
from duneapi.util import open_query

from dune_api_scripts.local_env import DUNE_DATA_DIR, QUERY_DIR, DUNE_CONNECTION
from dune_api_scripts.update.utils import update_args, Environment, refresh
from dune_api_scripts.utils import date_range


@dataclass
class Retention:
    """Retention values for date"""

    day: date
    retained: int
    hybrid: int
    lost: int
    gone: int

    @classmethod
    def from_dict(cls, obj: dict[str, str]) -> Retention:
        """Constructs retention record from Dune Data as string dict"""
        return cls(
            day=datetime.strptime(obj["day"], "%Y-%m-%d").date(),
            retained=int(obj["retained"]),
            hybrid=int(obj["hybrid"]),
            lost=int(obj["lost"]),
            gone=int(obj["gone"]),
        )

    def __str__(self) -> str:
        return f"('{self.day}',{self.retained},{self.hybrid},{self.lost},{self.gone})"


def fetch_retention_for_date(
    dune: DuneAPI, query_filepath: str, day: date
) -> Retention:
    """Initiates and executes Dune query, returning results as Python Objects"""
    formatted_date = f"{str(day)} 00:00:00"
    retention_query = DuneQuery.from_environment(
        raw_sql=open_query(query_filepath),
        name="Retention on Day",
        network=Network.MAINNET,
        parameters=[
            QueryParameter.date_type("DateFor", formatted_date),
            QueryParameter.number_type("NumDays", 30),
        ],
    )
    result = dune.fetch(retention_query)
    assert len(result) == 1
    return Retention.from_dict(result[0])


def update_retention_view(
    dune: DuneAPI, query_filepath: str, values: list[Retention], env: Environment
) -> None:
    """Updates user generated view with retention values"""
    raw_sql = open_query(query_filepath).replace(
        "{{Values}}",
        ",\n             ".join(map(str, values)),
    )
    query = DuneQuery(
        raw_sql=raw_sql,
        name="User Retention",
        description="Values for Cow Protocol User Retention",
        parameters=[env.as_query_param()],
        network=Network.MAINNET,
        query_id=int(os.environ.get("RETENTION_QUERY", 1103196)),
    )
    refresh(dune, query)


def open_or_create(path: str, file: str) -> tuple[list[Retention], date]:
    """Opens csv with retention data or creates a new one."""
    existing_data = []
    filename = os.path.join(path, file)
    try:
        with open(filename, "r", encoding="utf-8") as retention_file:
            reader = csv.DictReader(retention_file)
            latest_entry = date(year=2021, month=5, day=28)
            for row in reader:
                record = Retention.from_dict(row)
                if record.day > latest_entry:
                    latest_entry = record.day
                existing_data.append(record)
    except FileNotFoundError:
        print(f"No file found at {filename}")
        if not os.path.exists(path):
            os.makedirs(path)
        with open(filename, "a", encoding="utf-8") as new_file:
            new_file.write("day,retained,hybrid,lost,gone")
        # First official day of 30 day retention
        latest_entry = datetime.strptime("2021-05-28", "%Y-%m-%d").date()

    return existing_data, latest_entry


def fetch_retention_till(
    dune: DuneAPI, end: date, retention_file_path: str, env: Environment
) -> None:
    """Method that loads existing retention data and fetches the rest by day"""
    existing_data, latest_entry = open_or_create(DUNE_DATA_DIR, "retention.csv")

    start = latest_entry + timedelta(days=1)
    missing_dates = date_range(start, end)
    print(f"Fetching Retention from {start} to {end} (yesterday)")

    for day in missing_dates:
        print(f"fetching retention data for day {str(day)}")
        day_result = fetch_retention_for_date(
            dune, query_filepath=f"{QUERY_DIR}/retention-on-date.sql", day=day
        )
        existing_data.append(day_result)
        print("Got results", day_result)
        with open(retention_file_path, "a", encoding="utf-8") as csv_file:
            csv_file.write("\n" + ",".join([str(t) for t in astuple(day_result)]))

    update_retention_view(
        dune,
        query_filepath=f"{QUERY_DIR}/retention-complete.sql",
        values=existing_data,
        env=env,
    )


if __name__ == "__main__":
    fetch_retention_till(
        dune=DUNE_CONNECTION,
        # use one day before today since today's values aren't yet finalized.
        end=datetime.today().date() - timedelta(days=1),
        retention_file_path="/".join([DUNE_DATA_DIR, "retention.csv"]),
        env=update_args().environment,
    )
