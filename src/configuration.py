import logging
from pydantic import BaseModel, Field, ValidationError
from keboola.component.exceptions import UserException


class Db(BaseModel):
    workspace_id: int | None = Field(alias="workspaceId", default=None)


class ColumnSpec(BaseModel):
    name: str
    dbName: str
    type: str
    nullable: bool
    default: str = ""
    size: str = ""


class Configuration(BaseModel):
    db: Db = Field(default_factory=Db)
    table_id: str = Field(alias="tableId", default="")
    incremental: bool = False
    destination_table_name: str = Field(alias="dbName", default="")
    preserve_existing_tables: bool = True
    debug: bool = False
    items: list[ColumnSpec] = []
    clone: bool = False

    def __init__(self, **data):
        try:
            super().__init__(**data)
        except ValidationError as e:
            error_messages = [f"{err['loc'][0]}: {err['msg']}" for err in e.errors()]
            raise UserException(f"Validation Error: {', '.join(error_messages)}")

        if self.debug:
            logging.debug("Component will run in Debug mode")
