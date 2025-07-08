import logging
from pydantic import BaseModel, Field, ValidationError
from keboola.component.exceptions import UserException


class Keys(BaseModel):
    private: str = Field(alias="#private")
    public: str


class SSH(BaseModel):
    keys: Keys = Field(default=Keys)


class ColumnSpec(BaseModel):
    name: str
    dbName: str
    type: str
    nullable: bool
    default: str = ""
    size: str = ""


class Configuration(BaseModel):
    ssh: SSH = Field(default_factory=SSH)
    table_id: str = Field(alias="tableId")
    destination_table_name: str = Field(alias="dbName")
    preserve_existing_tables: bool = True
    debug: bool = False
    items: list[ColumnSpec]
    clone: bool = False

    def __init__(self, **data):
        try:
            super().__init__(**data)
        except ValidationError as e:
            error_messages = [f"{err['loc'][0]}: {err['msg']}" for err in e.errors()]
            raise UserException(f"Validation Error: {', '.join(error_messages)}")

        if self.debug:
            logging.debug("Component will run in Debug mode")
