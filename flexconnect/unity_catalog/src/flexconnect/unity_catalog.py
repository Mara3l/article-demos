# (C) 2024 GoodData Corporation
import os
from typing import Optional

import gooddata_flight_server as gf
import polars as pl
import pyarrow
import structlog
from databricks import sql
from databricks.sdk import WorkspaceClient
from gooddata_flexconnect import (
    ExecutionContext,
    FlexConnectFunction,
)

_LOGGER = structlog.get_logger("UnityCatalog_FlexConnect")

TOKEN = os.getenv("UNITY_CATALOG_TOKEN")
HOST = os.getenv("UNITY_CATALOG_HOST")
HTTP_PATH = os.getenv("UNITY_CATALOG_HTTP_PATH")
TABLE_NAME = os.getenv("UNITY_CATALOG_TABLE_NAME")

class UnityCatalogFunction(FlexConnectFunction):
    """FlexConnect function implementation for Unity Catalog.

    This class interfaces with Databricks Unity Catalog to fetch data
    and provide it in a format compatible with GoodData Flight Server.
    """
    Name = "UnityCatalog_FlexConnect"

    type_mappings = {
        'STRING': pyarrow.string(),
        'INT': pyarrow.int32(),
        'INTEGER': pyarrow.int32(),
        'BIGINT': pyarrow.float64(),
        'FLOAT': pyarrow.float32(),
        'DOUBLE': pyarrow.float64(),
        'BOOLEAN': pyarrow.bool_(),
        'TIMESTAMP': pyarrow.timestamp('ms'),
        'DATE': pyarrow.date32(),
    }
    Schema = {}

    def __init__(self):
        """Initializes the UnityCatalogFunction.

        Connects to the Databricks Workspace and constructs the schema based
        on the specified table in Unity Catalog.
        """
        w = WorkspaceClient(host=HOST,token=TOKEN)
        table = w.tables.get(TABLE_NAME)

        # Extract column information
        columns = table.columns

        fields = []
        for col in columns:
            name = col.name
            data_type = col.type_text.upper()
            nullable = col.nullable if hasattr(col, 'nullable') else True

            if data_type in self.type_mappings:
                arrow_type = self.type_mappings[data_type]
            else:
                raise TypeError(f"Unsupported data type: {data_type}")

            fields.append(pyarrow.field(name, arrow_type, nullable=nullable))

        self.Schema = pyarrow.schema(fields)

    def call(
        self,
        parameters: dict,
        columns: Optional[tuple[str, ...]],
        headers: dict[str, list[str]],
    ) -> gf.ArrowData:
        """Executes the FlexConnect function call.
        Basically this executes an SQL against Unity Catalog. You can easily extend this with any business logic.

        Args:
            parameters (dict): Parameters provided for the function execution.
            columns (Optional[tuple[str, ...]]): Columns to be selected.
            headers (dict[str, list[str]]): HTTP headers from the request.

        Returns:
            gf.ArrowData: The data fetched from the Unity Catalog table as Arrow data.

        Raises:
            ValueError: If execution context is not provided.
        """
        _LOGGER.info("function_called", parameters=parameters)

        execution_context = ExecutionContext.from_parameters(parameters)
        if execution_context is None:
            # This can happen for invalid invocations that do not come from GoodData
            raise ValueError("Function did not receive execution context.")

        _LOGGER.info("execution_context", execution_context=execution_context)

        with sql.connect(
            server_hostname=HOST,
            http_path=HTTP_PATH,
            access_token=TOKEN
        ) as connection:

            selected_columns = ", ".join(columns) if columns else "*"

            query = f"SELECT {selected_columns} FROM {TABLE_NAME}"
            cursor = connection.cursor()
            cursor.execute(query)
            data = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            pl_df = pl.DataFrame(data, schema=column_names, orient="row")
            _LOGGER.info("pl_df", table=pl_df.to_arrow())
            return pl_df.to_arrow()

    @staticmethod
    def on_load(ctx: gf.ServerContext) -> None:
        pass
