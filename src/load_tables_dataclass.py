from pydantic import BaseModel, Field, computed_field
from typing import Optional


class BaseConfigModel(BaseModel):
    class Config:
        validate_by_name = True


class Column(BaseConfigModel):
    source: str
    destination: Optional[str] = None
    type: Optional[str] = None
    length: Optional[str] = None
    nullable: bool = True
    convert_empty_values_to_null: bool = Field(default=False, alias="convertEmptyValuesToNull")


class Table(BaseConfigModel):
    source: str
    destination: str
    where_column: Optional[str] = Field(default=None, alias="whereColumn")
    where_values: list[str] = Field(default_factory=list, alias="whereValues")
    where_operator: Optional[str] = Field(default=None, alias="whereOperator")
    columns: Optional[list[Column] | list[str]] = Field(default_factory=list)
    keep_internal_timestamp_column: bool = Field(default=False, exclude=True)
    overwrite: bool = Field(default=True, alias="overwrite")
    incremental: bool = Field(default=False)

    @computed_field(alias="dropTimestampColumn", return_type=bool)
    @property
    def drop_timestamp_column(self) -> bool:
        return not self.keep_internal_timestamp_column


class StorageInput(BaseModel):
    tables: list[Table] = Field(default=Table)
