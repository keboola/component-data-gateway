from pydantic import BaseModel, Field, computed_field


class BaseConfigModel(BaseModel):
    class Config:
        validate_by_name = True


class PrimaryKey(BaseConfigModel):
    autority: str = "manual"
    columns: list[str] | None = Field(default_factory=list)


class Column(BaseConfigModel):
    source: str
    destination: str | None = None
    type: str | None = None
    length: str | None = None
    nullable: bool = True
    convert_empty_values_to_null: bool = Field(default=False, alias="convertEmptyValuesToNull")


class Table(BaseConfigModel):
    source: str
    destination: str
    where_column: str | None = Field(default=None, alias="whereColumn")
    where_values: list[str] = Field(default_factory=list, alias="whereValues")
    where_operator: str = Field(default="eq", alias="whereOperator")
    columns: list[Column] | list[str] | None = Field(default_factory=list)
    overwrite: bool = Field(default=True, alias="overwrite")
    incremental: bool = Field(default=False)
    seconds: int | None = None
    changed_since: str | None = Field(default=None, alias="changedSince")
    primary_key: PrimaryKey = Field(default_factory=PrimaryKey, alias="primaryKey")


class StorageInput(BaseModel):
    tables: list[Table] = Field(default_factory=list)
