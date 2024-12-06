from dagster import Definitions
from my_dagster_project.assets import (
    loading_data2,
    connect_to_mongodb2,
    insert_data_into_mongodb2,
    fetching_data2,
    cleaning_data2,
    connect_to_postgres2,
    insert_data_into_postgres2,
    fetching_data_from_postgres2,
)

defs = Definitions(
    assets=[
        loading_data2,
        connect_to_mongodb2,
        insert_data_into_mongodb2,
        fetching_data2,
        cleaning_data2,
        connect_to_postgres2,
        insert_data_into_postgres2,
        fetching_data_from_postgres2,
    ]
)
