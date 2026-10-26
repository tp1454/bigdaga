"""Thin wrapper around the ClickHouse client for batched inserts."""

import clickhouse_connect

from . import config


class ClickHouseWriter:
    def __init__(self):
        self.client = clickhouse_connect.get_client(
            host=config.CLICKHOUSE_HOST,
            port=config.CLICKHOUSE_PORT,
            username=config.CLICKHOUSE_USER,
            password=config.CLICKHOUSE_PASSWORD,
            database=config.CLICKHOUSE_DB,
        )

    def insert(self, table, columns, rows):
        self.client.insert(table, rows, column_names=columns)

    def close(self):
        self.client.close()
