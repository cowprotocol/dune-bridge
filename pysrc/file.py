"""File Reader and Writer for DuneRecords"""
from __future__ import annotations

import logging
import os.path
from pathlib import Path

# ndjson missing types: https://github.com/rhgrant10/ndjson/issues/10
import ndjson  # type: ignore
from dune_client.types import DuneRecord

from pysrc.environment import OUT_DIR

logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s %(message)s")


class FileIO:
    """
    CSV is a more compact file type but requires iteration over the set pre and post write
    JSON is a redundant file format but writes the content as it is received from Dune.
    """

    def __init__(self, path: Path | str = OUT_DIR):
        self.path = path

    def write(self, data: list[DuneRecord], name: str) -> None:
        """Writes `data` to file `name`"""
        if len(data) == 0:
            logger.warning(f"Nothing to write... skipping {name}")
            return

        if not os.path.exists(self.path):
            os.makedirs(self.path)

        filename = os.path.join(self.path, name)

        with open(filename, "w", encoding="utf-8") as file:
            writer = ndjson.writer(file, ensure_ascii=False)
            for row in data:
                writer.writerow(row)

    def read(self, name: str) -> list[DuneRecord]:
        """Reads DuneRecords from file `name`"""
        filename = os.path.join(self.path, name)
        logger.debug(f"Attempting to loading data from file {filename}")
        with open(filename, "r", encoding="utf-8") as file:
            reader = ndjson.reader(file)
            return list(reader)
