from __future__ import annotations

import logging
import re

from soda.cloud.dbt_config import DbtCloudConfig
from soda.common.logs import Logs
from soda.common.parser import Parser
from soda.configuration.configuration import Configuration
from soda.sampler.soda_cloud_sampler import SodaCloudSampler
from soda.soda_cloud.soda_cloud import SodaCloud

logger = logging.getLogger(__name__)

DATA_SOURCE = "data_source"
CONNECTION = "connection"
SODA_CLOUD = "soda_cloud"
DBT_CLOUD_API_TOKEN = "dbt_cloud"


class ConfigurationParser(Parser):
    def __init__(self, configuration: Configuration, logs: Logs, file_path: str):
        super().__init__(file_path=file_path, logs=logs)

        self.configuration: Configuration = configuration

    def parse_environment_yaml_str(self, environment_yaml_str: str):
        environment_yaml_str = self._resolve_jinja(environment_yaml_str)
        environment_dict = self._parse_yaml_str(environment_yaml_str)
        if environment_dict is not None:
            self.__parse_headers(environment_dict)

    def __parse_headers(self, environment_dict: dict) -> None:
        if not environment_dict:
            return

        for environment_header, header_value in environment_dict.items():
            if environment_header.startswith(f"{DATA_SOURCE} "):
                self._push_path_element(environment_header, header_value)
                data_source_name = environment_header[len(f"{DATA_SOURCE} ") :].strip()
                if not re.compile(r"^[a-z_][a-z_0-9]+$").match(data_source_name):
                    self.logs.error(
                        f"Invalid data source name '{data_source_name}'. Data source names must "
                        f"start with a lower case char or an underscore [a-z_], followed by any "
                        f"number of lower case chars, digits or underscore [a-z0-9_]"
                    )
                self.configuration.data_source_properties_by_name[data_source_name] = header_value

                self._get_required("type", str)

                # Backward compatibility. Merge connection properties one level up, into the data source properties. DS properties take precedence.
                data_source_connection = header_value.get("connection")
                if data_source_connection:
                    if isinstance(data_source_connection, dict):
                        for k, v in data_source_connection.items():
                            if k not in self.configuration.data_source_properties_by_name[data_source_name]:
                                self.configuration.data_source_properties_by_name[data_source_name][k] = v
                    else:
                        self.logs.error(
                            "connection must be a dict",
                            location=self.location,
                        )
                self._pop_path_element()

            elif environment_header == SODA_CLOUD:
                self._push_path_element(SODA_CLOUD, header_value)
                self.configuration.soda_cloud = self.parse_soda_cloud_cfg(header_value)
                if self.configuration.soda_cloud and not header_value.get("disable_samples"):
                    self.configuration.sampler = SodaCloudSampler()
                self._pop_path_element()

            elif environment_header == DBT_CLOUD_API_TOKEN:
                self._push_path_element(DBT_CLOUD_API_TOKEN, header_value)
                self.configuration.dbt_cloud = self.parse_dbt_cloud_cfg(header_value)
                self._pop_path_element()

            else:
                self.logs.error(
                    f'Invalid configuration header: expected "{DATA_SOURCE} {{data source name}}".',
                    location=self.location,
                )

    def parse_soda_cloud_cfg(self, config_dict: dict):
        api_key = config_dict.get("api_key_id")
        api_secret = config_dict.get("api_key_secret")
        host = None
        if "host" in config_dict:
            host = config_dict.get("host")
        port = None
        if "port" in config_dict:
            port = config_dict.get("port")
        return SodaCloud(
            api_key_id=api_key, api_key_secret=api_secret, host=host, token=None, port=port, logs=self.logs
        )

    def parse_dbt_cloud_cfg(self, config_dict: dict):
        api_token = config_dict.get("api_token")
        account_id = config_dict.get("account_id")

        return DbtCloudConfig(api_token=api_token, account_id=account_id)
