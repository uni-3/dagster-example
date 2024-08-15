from dagster_duckdb import DuckDBResource
from dagster import EnvVar, resource
import airbyte as ab

database_resource = DuckDBResource(
    database=EnvVar("DUCKDB_DATABASE"),
)

# pyairbyteのキャッシュリソース
@resource
def pyairbyte_cache_resource(_):
    return ab.get_default_cache()
