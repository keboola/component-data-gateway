"""
Template Component main class.

"""

import logging
import time

from kbcstorage.client import Client
from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

from configuration import Configuration
from load_tables_dataclass import Column, StorageInput


class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        self.params = Configuration(**self.configuration.parameters)
        self.storage_input = StorageInput(**self.configuration.config_data.get("storage", {}).get("input"))
        self.client = Client(
            self.environment_variables.url,
            self.environment_variables.token,
            self.environment_variables.branch_id,
        )

    def run(self):
        workspace_id = self.client.configurations.list_config_workspaces(
            "keboola.app-data-gateway",
            self.environment_variables.config_id,
        )[-1].get("id")

        table_mapping = self.build_table_mapping()

        job = self.client.workspaces.load_tables(
            workspace_id=workspace_id,
            table_mapping=table_mapping,
            preserve=self.params.preserve_existing_tables,
            load_type="load",
        )

        while True:
            job = self.client.jobs.detail(job["id"])
            if job["status"] in ["success", "error"]:
                break
            logging.info(f"Job {job['id']} is still running, status: {job['status']}")
            time.sleep(5)

        match job["status"]:
            case "error":
                raise UserException(f"Job {job['id']} failed with error: {job.get('error', {}).get('message')}")
            case "success":
                logging.info(f"Job {job['id']} finished successfully.")

    def build_table_mapping(self) -> list[dict]:
        """
        Combines the input table with the columns specified in the configuration.
        """
        selected_table = [table for table in self.storage_input.tables if table.source == self.params.table_id]

        for column in self.params.items:
            selected_table[0].columns.append(
                Column(
                    source=column.name,
                    destination=column.dbName,
                    type=column.type,
                    length=column.size,
                    nullable=column.nullable,
                    convert_empty_values_to_null=True,
                )
            )

        in_table = StorageInput(tables=selected_table).model_dump(by_alias=True)["tables"]
        in_table[0].pop("dropTimestampColumn")

        if not in_table[0].get("whereColumn"):
            in_table[0].pop("whereColumn")
            in_table[0].pop("whereOperator")

        return in_table


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
