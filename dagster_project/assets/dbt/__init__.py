from pathlib import Path

from dagster import AssetExecutionContext, AssetKey, file_relative_path
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, dbt_assets

DBT_PROJECT_DIR = file_relative_path(__file__, "../../../dbt_project")

dbt_resource = DbtCliResource(project_dir=DBT_PROJECT_DIR)
dbt_parse_invocation = dbt_resource.cli(["--quiet", "parse"], target_path=Path("target")).wait()
dbt_manifest_path = dbt_parse_invocation.target_path.joinpath("manifest.json")


class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    @classmethod
    def get_asset_key(cls, dbt_resource_props):
        type = dbt_resource_props["resource_type"]
        name = dbt_resource_props["name"]
        if type == "source":
            return AssetKey(f"{name}")
        else:
            return super().get_asset_key(cls, dbt_resource_props)

    @classmethod
    def get_metadata(cls, dbt_node_info):
        return {
            "columns": dbt_node_info["columns"],
            "sources": dbt_node_info["sources"],
            "description": dbt_node_info["description"],
        }

    @classmethod
    def get_group_name(cls, dbt_resource_props):
        return "dbt"


@dbt_assets(
    manifest=dbt_manifest_path,
    dagster_dbt_translator=CustomDagsterDbtTranslator(),
    name="dbt"
)
def dbt_project_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
