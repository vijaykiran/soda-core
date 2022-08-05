from __future__ import annotations

from collections import defaultdict
from functools import reduce
from operator import or_
from pathlib import Path
from typing import Any, Optional

from dbt.contracts.graph.compiled import (
    CompiledModelNode,
    CompiledGenericTestNode,
    CompiledSeedNode,
)
from dbt.contracts.graph.parsed import (
    ParsedModelNode,
    ParsedGenericTestNode,
    ParsedSeedNode,
    ParsedSourceDefinition,
)
from dbt.contracts.results import RunResultOutput
from dbt.node_types import NodeType
from soda.cloud.dbt_config import DbtCloudConfig
from soda.scan import Scan
from soda.soda_cloud.soda_cloud import SodaCloud
import json
from typing import Iterator
from soda.model.dataset import Dataset
from requests.structures import CaseInsensitiveDict
import requests


class DbtCloud:
    ADMIN_API_BASE_URL = "https://cloud.getdbt.com/api/v2/accounts/"

    def __init__(
        self,
        scan: Scan,
        dbt_artifacts: Path | None = None,
        dbt_manifest: Path | None = None,
        dbt_run_results: Path | None = None,
        dbt_cloud_run_id: str | None = None,
        dbt_cloud_job_id: str | None = None,
    ) -> None:
        self.scan: Scan = scan
        self.soda_cloud: SodaCloud = scan._configuration.soda_cloud
        self.dbt_cloud_config: DbtCloudConfig = scan._configuration.dbt_cloud
        self.dbt_artifacts = dbt_artifacts
        self.dbt_manifest = dbt_manifest
        self.dbt_run_results = dbt_run_results
        self.dbt_cloud_run_id = dbt_cloud_run_id
        self.dbt_cloud_job_id = dbt_cloud_job_id

    def ingest(self):
        return_code = 0

        if self.dbt_artifacts or self.dbt_manifest or self.dbt_run_results:
            self.scan._logs.info("Using local dbt artifacts.")

            if self.dbt_artifacts:
                dbt_manifest = self.dbt_artifacts / "manifest.json"
                dbt_run_results = self.dbt_artifacts / "run_results.json"

            if not dbt_manifest or not dbt_manifest.is_file():
                raise ValueError(
                    f"dbt manifest ({dbt_manifest}) or artifacts ({self.dbt_artifacts}) "
                    "should point to an existing path."
                )
            elif dbt_run_results is None or not dbt_run_results.is_file():
                raise ValueError(
                    f"dbt run results ({dbt_run_results}) or artifacts ({self.dbt_artifacts}) "
                    "should point to an existing path."
                )

            manifest, run_results = self._load_dbt_artifacts(
                dbt_manifest,
                dbt_run_results,
            )
        else:
            self.scan._logs.info("Getting dbt artifacts from dbt Cloud.")

            error_values = [self.dbt_cloud_config.api_token, self.dbt_cloud_config.account_id]
            error_messages = [
                f"Expecting a dbt cloud api token: {self.dbt_cloud_config.api_token}",
                f"Expecting a dbt cloud account id: {self.dbt_cloud_config.account_id}",
            ]
            filtered_messages = [message for value, message in zip(error_values, error_messages) if value is None]

            if not self.dbt_cloud_run_id and not self.dbt_cloud_job_id:
                filtered_messages.append("Expecting either a dbt cloud job id, or run id. None are provided.")

            if len(filtered_messages) > 0:
                raise ValueError("\n".join(filtered_messages))

            manifest, run_results = self._download_dbt_manifest_and_run_results(
                self.dbt_cloud_config.api_token,
                self.dbt_cloud_config.account_id,
                self.dbt_cloud_run_id,
                self.dbt_cloud_job_id,
            )

        test_results_iterator = self._map_dbt_test_results_iterator(manifest, run_results)

        self.flush_test_results(
            test_results_iterator,
            soda_server_client,
            warehouse_name=warehouse_yml.name,
            warehouse_type=warehouse_yml.dialect.type,
        )

        return return_code

    def _load_dbt_artifacts(
        manifest_file: Path,
        run_results_file: Path,
    ) -> tuple[dict, dict]:
        with manifest_file.open("r") as file:
            manifest = json.load(file)
        with run_results_file.open("r") as file:
            run_results = json.load(file)

        return manifest, run_results

    def _download_dbt_manifest_and_run_results(
        self,
        api_token: str,
        account_id: str,
        run_id: Optional[str],
        job_id: Optional[str],
    ) -> tuple[dict, dict]:
        manifest = self._download_dbt_artifact_from_cloud("manifest.json", api_token, account_id, run_id, job_id)
        run_results = self._download_dbt_artifact_from_cloud("run_results.json", api_token, account_id, run_id, job_id)

        return manifest, run_results

    def _map_dbt_test_results_iterator(
        self, manifest: dict, run_results: dict
    ) -> Iterator[tuple[Dataset, list[TestResult]]]:
        """
        Create an iterator for the dbt test results.

        Parameters
        ----------
        manifest : dict
            The manifest.
        run_results : dict
            The run results

        Returns
        -------
        out : Iterator[tuple[Table, list[TestResult]]]
            The table and its corresponding test results.
        """
        model_nodes, seed_nodes, test_nodes, source_nodes = self._parse_manifest(manifest)
        parsed_run_results = self._parse_run_results(run_results)

        tests_with_test_result = self._map_dbt_run_result_to_test_result(test_nodes, parsed_run_results)

        model_seed_and_source_nodes = {**model_nodes, **seed_nodes, **source_nodes}

        models_with_tests = soda_dbt.create_nodes_to_tests_mapping(
            model_seed_and_source_nodes, test_nodes, parsed_run_results
        )

        for unique_id, test_unique_ids in models_with_tests.items():
            node = model_seed_and_source_nodes[unique_id]
            table = Dataset(
                node.name if isinstance(node, ParsedSourceDefinition) else node.alias,
                node.database,
                node.schema,
            )
            test_results = [tests_with_test_result[test_unique_id] for test_unique_id in test_unique_ids]

            yield table, test_results

    def _map_dbt_run_result_to_test_result(
        self,
        test_nodes: dict[str, "DbtTestNode"] | None,
        run_results: list["RunResultOutput"],
    ) -> dict[str, set["DbtModelNode"]]:
        from dbt.contracts.results import TestStatus

        assert (
            test_nodes is not None
        ), "No test nodes were retrieved from the manifest.json. This could be because no tests have been implemented in dbt yet or you never ran `dbt test`."
        dbt_tests_with_soda_test = {
            test_node.unique_id: Test(
                id=test_node.unique_id,
                title=f"{test_node.name}",
                expression=test_node.raw_sql,
                metrics=None,
                column=test_node.column_name,
                source="dbt",
            )
            for test_node in test_nodes.values()
        }

        tests_with_test_result = {
            run_result.unique_id: TestResult(
                dbt_tests_with_soda_test[run_result.unique_id],
                passed=run_result.status == TestStatus.Pass,
                skipped=run_result.status == TestStatus.Skipped,
                values={"failures": run_result.failures},
            )
            for run_result in run_results
            if run_result.unique_id in test_nodes.keys()
        }
        return tests_with_test_result

    def _download_dbt_artifact_from_cloud(
        self,
        artifact: str,
        api_token: str,
        account_id: str,
        run_id: Optional[str],
        job_id: Optional[str],
    ) -> dict:
        """
        Download an artifact from the dbt cloud by run_id. If a job_id is provided instead of
        a run_id the latest run_id available for that job will first be fetched and used to download
        the artifact.

        https://docs.getdbt.com/dbt-cloud/api-v2#operation/getArtifactsByRunId
        """

        if job_id is not None:
            self.scan_logs.info(f"Retrieving latest run for job: {job_id}")
            run_id = self._get_latest_run_id(api_token, account_id, job_id)

            assert run_id, "Could not get a valid run_id for this job"
        elif run_id is not None:
            pass
        else:
            raise AttributeError(
                "Either a dbt run_id or a job_id must be provided. If a job_id is provided "
                "soda ingest will fetch the latest available run artifacts from dbt Cloud for that job_id."
            )
        url = f"{self.ADMIN_API_BASE_URL}{account_id}/runs/{run_id}/artifacts/{artifact}"

        headers = CaseInsensitiveDict()
        headers["Authorization"] = f"Token {api_token}"
        headers["Content-Type"] = "application/json"

        self.scan_logs.info(f"Downloading artifact: {artifact}, from run: {run_id}")

        response = requests.get(url, headers=headers)
        if response.status_code != requests.codes.ok:
            response.raise_for_status()

        return response.json()

    def _get_latest_run_id(self, api_token: str, account_id: str, job_id: str) -> Optional[str]:
        url = f"{self.ADMIN_API_BASE_URL}{account_id}/runs"

        headers = CaseInsensitiveDict()
        headers["Authorization"] = f"Token {api_token}"
        headers["Content-Type"] = "application/json"

        query_params = {"job_definition_id": job_id, "order_by": "-finished_at"}
        response = requests.get(url, headers=headers, params=query_params)

        if response.status_code != requests.codes.ok:
            response.raise_for_status()

        response_json = response.json()
        run_id = response_json.get("data")[0].get("id")

        return run_id

    def _parse_manifest(
        self, manifest: dict[str, Any]
    ) -> tuple[
        dict[str, ParsedModelNode | CompiledModelNode] | None,
        dict[str, ParsedSeedNode | CompiledSeedNode] | None,
        dict[str, ParsedGenericTestNode | CompiledGenericTestNode] | None,
        dict[str, ParsedSourceDefinition] | None,
    ]:
        """
        Parse the manifest.

        Only V6 manifest is supported.

        https://docs.getdbt.com/reference/artifacts/manifest-json
        """
        if manifest.get("nodes") is not None:
            model_nodes = {
                node_name: CompiledModelNode(**node) if "compiled" in node.keys() else ParsedModelNode(**node)
                for node_name, node in manifest["nodes"].items()
                if node["resource_type"] == NodeType.Model
            }
            seed_nodes = {
                node_name: CompiledSeedNode(**node) if "compiled" in node.keys() else ParsedSeedNode(**node)
                for node_name, node in manifest["nodes"].items()
                if node["resource_type"] == NodeType.Seed
            }

            test_nodes = {}
            for node_name, node in manifest["nodes"].items():
                if node["resource_type"] == NodeType.Test:
                    if "test_metadata" in node.keys():
                        if "compiled" in node.keys():
                            node = CompiledGenericTestNode(**node)
                        else:
                            node = ParsedGenericTestNode(**node)
                        test_nodes[node_name] = node
                    else:
                        # TODO: ??????????????????? COrect indent???
                        self.scan._logs.info(f"Ignoring unsupported {node_name}")

        else:
            model_nodes = None
            seed_nodes = None
            test_nodes = None

        if manifest.get("sources") is not None:
            source_nodes: Optional[dict] = {
                source_name: ParsedSourceDefinition(**source)
                for source_name, source in manifest["sources"].items()
                if source["resource_type"] == NodeType.Source
            }
        else:
            source_nodes = None

        return model_nodes, seed_nodes, test_nodes, source_nodes

    def _parse_run_results(self, run_results: dict[str, Any]) -> list[RunResultOutput]:
        """
        Parse the run results.

        Only V4 run results is supported.

        https://docs.getdbt.com/reference/artifacts/run-results-json
        """
        parsed_run_results = [RunResultOutput(**result) for result in run_results["results"]]

        self._all_test_failures_are_not_none(parsed_run_results)

        return parsed_run_results

    def _create_nodes_to_tests_mapping(
        self,
        model_nodes: dict[str, ParsedModelNode],
        test_nodes: dict[str, CompiledGenericTestNode | ParsedGenericTestNode] | None,
        run_results: list[RunResultOutput],
    ) -> dict[str, set[ParsedModelNode]]:
        """
        Map models to tests.

        Parameters
        ----------
        model_nodes : Dict[str: ParsedModelNode]
            The parsed model nodes.
        test_nodes : Dict[str: CompiledGenericTestNode]
            The compiled schema test nodes.
        run_results : List[RunResultOutput]
            The run results.

        Returns
        -------
        out : Dict[str, set[ParseModelNode]]
            A mapping from models to tests.
        """
        assert (
            test_nodes is not None
        ), "No test nodes found in manifest.json. This could be because no test was implemented in dbt yet"

        test_unique_ids = [
            run_result.unique_id for run_result in run_results if run_result.unique_id in test_nodes.keys()
        ]

        models_that_tests_depends_on = {
            test_unique_id: set(test_nodes[test_unique_id].depends_on["nodes"]) for test_unique_id in test_unique_ids
        }

        model_unique_ids = reduce(
            or_,
            [model_unique_ids for model_unique_ids in models_that_tests_depends_on.values()],
        )

        models_with_tests = defaultdict(set)
        for model_unique_id in model_unique_ids:
            for (
                test_unique_id,
                model_unique_ids_of_test,
            ) in models_that_tests_depends_on.items():
                if model_unique_id in model_unique_ids_of_test:
                    models_with_tests[model_unique_id].add(test_unique_id)

        return models_with_tests

    def _all_test_failures_are_not_none(run_results: list[RunResultOutput]) -> bool:
        results_with_null_failures = []
        for run_result in run_results:
            if run_result.failures is None:
                results_with_null_failures.append(run_result)

        if len(results_with_null_failures) == len(run_results):
            raise ValueError(
                "Could not find a valid test result in the run results. "
                "This is often the case when ingesting from dbt Cloud where the last step in the "
                "job was neither a `dbt build` or `dbt test`. For example, your run may have terminated with "
                "`dbt docs generate` \n"
                "We are currently investigating this with the dbt Cloud team. \n"
                "In the meantime, if your jobs do not end on the above mentioned commands, you could make sure to add at least a `dbt test` "
                "step as your last step and make sure that 'generate documentation' is not turned on in your job definition."
            )
        else:
            return True
