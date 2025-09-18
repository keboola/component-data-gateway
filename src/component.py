import json
import logging
import time
from datetime import datetime
from pathlib import Path

from kbcstorage.client import Client
from keboola.component.base import ComponentBase, sync_action
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import MessageType, ValidationResult
from requests import HTTPError

from configuration import Configuration
from load_tables_dataclass import Column, StorageInput


class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        self.params = Configuration(**self.configuration.parameters)
        self.storage_input = None
        self.client = Client(
            self.environment_variables.url,
            self.environment_variables.token,
            self.environment_variables.branch_id,
            file_storage_support=False,
        )

    def run(self):
        self.storage_input = StorageInput(**self.configuration.config_data.get("storage", {}).get("input"))

        start_timestamp = time.time()
        workspaces = self.client.configurations.list_config_workspaces(
            "keboola.app-data-gateway",
            self.environment_variables.config_id,
        )
        workspace_id = workspaces[-1].get("id")  # get the id of latest created workspace

        table_mapping = self.build_table_mapping()

        try:
            job = self.client.workspaces.load_tables(
                workspace_id=workspace_id,
                table_mapping=table_mapping,
                preserve=self.params.preserve_existing_tables,
                load_type="load-clone" if self.params.clone else "load",
            )

            for i in range(60):
                job = self.client.jobs.detail(job["id"])
                if job["status"] in ["success", "error"]:
                    break
                logging.info(f"Job {job['id']} is still running, status: {job['status']}")
                time.sleep(5)
                if i == 59:
                    raise UserException(f"Job {job['id']} is still running giving up waiting.")

            match job["status"]:
                case "error":
                    logging.debug(f"Table mapping: {table_mapping}")
                    raise UserException(f"Job {job['id']} failed with error: {job.get('error', {}).get('message')}")
                case "success":
                    created = datetime.fromisoformat(job["createdTime"])
                    start = datetime.fromisoformat(job["startTime"])
                    end = datetime.fromisoformat(job["endTime"])
                    logging.info(
                        f"Load of {self.params.destination_table_name} finished successfully. Queued for "
                        f"{(start - created).seconds} s and processed for {(end - start).seconds} s."
                    )

            self.write_state_file({"last_run": start_timestamp})

        except HTTPError as e:
            raise UserException(f"Loading table failed: {e.response.text}")
        except Exception as e:
            raise UserException(f"Loading table failed: {str(e)}")

    def build_table_mapping(self) -> list[dict]:
        """
        Combines the input table with the columns specified in the configuration.
        Table name from configuration will always match one of the input tables.
        """
        tbl = [table for table in self.storage_input.tables if table.source == self.params.table_id][0]
        tbl.destination = self.params.destination_table_name

        tbl.columns = []
        for column in self.params.items:
            tbl.columns.append(
                Column(
                    source=column.name,
                    destination=column.dbName,
                    type=column.type,
                    length=column.size,
                    nullable=column.nullable,
                    convert_empty_values_to_null=True,
                )
            )

        in_table = StorageInput(tables=[tbl]).model_dump(by_alias=True)["tables"]

        # dropTimestampColumn is accepted only by load-clone endpoint
        if not self.params.clone:
            in_table[0].pop("dropTimestampColumn")  # it's always list of one table to keep the structure of the API

        if not self.params.preserve_existing_tables:
            in_table[0].pop("overwrite")  # supported by API only if preserve is true

        return in_table

    @sync_action("clean_workspace")
    def clean_workspace(self):
        try:
            with open(Path.joinpath(Path(self.data_folder_path), "config.json"), "r") as config_file:
                config_id = json.load(config_file).get("configId")

            workspaces = self.client.configurations.list_config_workspaces("keboola.app-data-gateway", config_id)

            job = self.client.workspaces.load_tables(
                workspace_id=workspaces[-1].get("id"),
                table_mapping=[],
                preserve=False,
                load_type="load",
            )

        except Exception as e:
            return ValidationResult(f"{str(e)}", MessageType.ERROR)

        while True:
            job = self.client.jobs.detail(job["id"])
            if job["status"] in ["success", "error"]:
                break
            time.sleep(1)

        if job["status"] == "success":
            return ValidationResult("Workspace cleaned successfully", MessageType.SUCCESS)
        else:
            return ValidationResult(f"{job.get('error', {}).get('message')}", MessageType.ERROR)


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
