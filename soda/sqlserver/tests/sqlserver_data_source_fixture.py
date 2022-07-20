from __future__ import annotations

import logging
import os

from helpers.data_source_fixture import DataSourceFixture
from helpers.test_table import TestTable

logger = logging.getLogger(__name__)


class SqlServerDataSourceFixture(DataSourceFixture):
    def _build_configuration_dict(self, schema_name: str | None = None) -> dict:
        return {
            "data_source sqlserver": {
                "type": "sqlserver",
                "host": "localhost",
                "username": os.getenv("SQLSERVER_USERNAME", "sodasql"),
                "password": os.getenv("SQLSERVER_PASSWORD"),
                "database": os.getenv("SQLSERVER_DATABASE", "sodasql"),
                "schema": schema_name or os.getenv("SQLSERVER_SCHEMA"),
            }
        }

    def _get_dataset_id(self):
        return f"{self.schema_data_source.project_id}.{self.schema_name}"

    def _create_schema_if_not_exists(self):
        # @TOOD
        pass

    def _drop_schema_if_exists(self):
        # @TOOD
        pass

    def _drop_test_table_sql(self, table_name):
        # @TOOD
        pass

    def _create_view_from_table_sql(self, test_table: TestTable):
        # @TOOD
        pass
