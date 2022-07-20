from __future__ import annotations

import json
import logging
from soda.common.logs import Logs

from soda.execution.data_source import DataSource
from soda.execution.data_type import DataType
from soda.common.exceptions import DataSourceConnectionError
import pyodbc

logger = logging.getLogger(__name__)


class SqlServerDataSource(DataSource):
    TYPE = "sqlserver"

    SCHEMA_CHECK_TYPES_MAPPING: dict = {
        # @TOOD
    }
    SQL_TYPE_FOR_CREATE_TABLE_MAP: dict = {
        # @TOOD
    }

    SQL_TYPE_FOR_SCHEMA_CHECK_MAP = {
        # @TOOD
    }

    # @TOOD
    NUMERIC_TYPES_FOR_PROFILING = ["NUMERIC", "INT64"]
    # @TOOD
    TEXT_TYPES_FOR_PROFILING = ["STRING"]

    def default_casify_datasource_type(self) -> str:
        return "SqlServer"

    def __init__(self, logs: Logs, data_source_name: str, data_source_properties: dict):
        super().__init__(logs, data_source_name, data_source_properties)

        self.host = data_source_properties.get("host", "localhost")
        self.port = data_source_properties.get("port", "1433")
        self.driver = data_source_properties.get("driver", "SQL Server")
        self.username = data_source_properties.get("username")
        self.password = data_source_properties.get("password")
        self.database = data_source_properties.get("database", "master")
        self.schema = data_source_properties.get("schema", "dbo")
        self.trusted_connection = data_source_properties.get("trusted_connection", False)
        self.encrypt = data_source_properties.get("encrypt", False)
        self.trust_server_certificate = data_source_properties.get("trust_server_certificate", False)

    def connect(self):
        try:
            self.connection = pyodbc.connect(
                ("Trusted_Connection=YES;" if self.trusted_connection else "")
                + ("TrustServerCertificate=YES;" if self.trust_server_certificate else "")
                + ("Encrypt=YES;" if self.encrypt else "")
                + "DRIVER={"
                + self.driver
                + "};SERVER="
                # + "SERVER="
                + self.host
                + ";PORT="
                + self.port
                + ";DATABASE="
                + self.database
                + ";UID="
                + self.username
                + ";PWD="
                + self.password
            )
            self.connection = dbapi.Connection(self.client)

            return self.connection
        except Exception as e:
            raise DataSourceConnectionError(self.TYPE, e)

    def safe_connection_data(self):
        return [
            self.type,
            self.host,
            self.port,
            self.database,
        ]
