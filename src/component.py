import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

from kbcstorage.client import Client
from keboola.component.base import ComponentBase, sync_action
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import MessageType, ValidationResult
from keboola.utils import get_past_date
from requests import HTTPError

from configuration import Configuration
from load_tables_dataclass import Column, StorageInput


class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        self.params = Configuration(**self.configuration.parameters)
        self.storage_input = None
        self.start_timestamp = None
        self.state = self.get_state_file()
        self.client = Client(
            self.environment_variables.url,
            self.environment_variables.token,
            self.environment_variables.branch_id,
            file_storage_support=False,
        )

    def run(self):
        self.storage_input = StorageInput(**self.configuration.config_data.get("storage", {}).get("input"))
        if not self.storage_input.tables:
            raise UserException("No tables found. Please add one to the input mapping.")

        self.start_timestamp = time.time()

        table_mapping = self.build_table_mapping()

        try:
            job = self.client.workspaces.load_tables(
                workspace_id=self.get_workspace_id(),
                table_mapping=table_mapping,
                preserve=self.params.preserve_existing_tables,
            )

            logging.debug(job)

            while True:
                job = self.client.jobs.detail(job["id"])
                if job["status"] in ["success", "error"]:
                    break
                logging.debug(f"Job {job['id']} is still running, status: {job['status']}")
                time.sleep(5)

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

            self.write_state_file({"last_run": self.start_timestamp})

        except HTTPError as e:
            raise UserException(f"Loading table failed: {e.response.text}")
        except Exception as e:
            raise UserException(f"Loading table failed: {str(e)}")

    def get_workspace_id(self) -> str:
        workspace_id = self.params.db.workspace_id

        if not workspace_id:  # fallback to old config version
            config_id = self.environment_variables.config_id

            if not config_id:  # for sync action
                with open(Path(self.data_folder_path) / "config.json") as config_file:
                    config_id = json.load(config_file).get("configId")

            workspaces = self.client.configurations.list_config_workspaces(
                "keboola.app-data-gateway",
                config_id=config_id,
            )

            if not workspaces:
                raise UserException("No workspaces found for this configuration, please create workspace first.")

            workspace_id = workspaces[-1].get("id")  # get the id of latest created workspace
        return workspace_id

    def get_since_seconds(self):
        changed_since = self.storage_input.tables[0].changed_since
        if changed_since == "adaptive":
            since = datetime.fromtimestamp(self.state.get("last_run") or 0, tz=timezone.utc)
        else:
            since = get_past_date(changed_since)

        delta = datetime.fromtimestamp(self.start_timestamp, tz=timezone.utc) - since
        return delta.seconds + 60  # to have reserve for component startup

    def build_table_mapping(self) -> list[dict]:
        """
        Combines the input table with the columns specified in the configuration.
        Table name from configuration will always match one of the input tables.
        """
        tbl = [table for table in self.storage_input.tables if table.source == self.params.table_id][0]
        tbl.destination = self.params.destination_table_name
        tbl.primary_key.columns = self.params.primary_key
        tbl.incremental = self.params.incremental

        if self.params.clone:
            tbl.load_type = "CLONE"

        if tbl.incremental:
            tbl.seconds = self.get_since_seconds()  # TODO: change for since + until when available
        else:
            tbl.overwrite = True

        tbl.columns = []
        for column in self.params.items:
            tbl.columns.append(
                Column(
                    source=column.name,
                    destination=column.dbName,
                    type=column.type,
                    length=column.size,
                    nullable=column.nullable,
                    convert_empty_values_to_null=column.nullable,  # design decision to use the same "nullable" param
                )
            )

        in_table = StorageInput(tables=[tbl]).model_dump(by_alias=True)["tables"]

        if not self.params.preserve_existing_tables or self.params.incremental:
            in_table[0].pop("overwrite")  # supported by API only if preserve is true

        in_table[0].pop("changedSince")  # TODO: remove when supported

        return in_table

    @sync_action("clean_workspace")
    def clean_workspace(self):
        try:
            job = self.client.workspaces.load_tables(
                workspace_id=self.get_workspace_id(),
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
