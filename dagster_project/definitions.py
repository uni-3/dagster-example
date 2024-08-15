# ref. https://github.com/dagster-io/dagster/blob/master/examples/assets_dbt_python/assets_dbt_python/__init__.py

from dagster import Definitions, load_assets_from_modules, AssetSelection, define_asset_job
from dagster import ScheduleDefinition
from dagster import (
    MonthlyPartitionsDefinition,
)
from dagster_dbt import build_dbt_asset_selection

from .assets import pyairbyte_project
from .assets.dbt import dbt_resource, dbt_project_assets
from .assets import dbt
from .resources import database_resource, pyairbyte_cache_resource

start_date = "2024-01-01"
end_date = "2024-10-01"

monthly_partition = MonthlyPartitionsDefinition(start_date=start_date, end_date=end_date)

all_asset_job = define_asset_job(
    name="all_job",
    partitions_def=monthly_partition,
    selection=AssetSelection.all()
)

dbt_dep_job = define_asset_job(
    name="transform_job",
    selection=build_dbt_asset_selection(
        [dbt_project_assets],
        dbt_select="tag:github_issues"
    )
)

pyairbyte_asset_job = define_asset_job(
    name="load_job",
    selection=AssetSelection.all() - AssetSelection.groups("dbt")
)


# TODO create schedule job with partition
# update_schedule = ScheduleDefinition(
#     job=all_asset_job,
#     cron_schedule="0 0 5 * *",  # every 5th of the month at midnight
# )

# all_schedules = [update_schedule]

github_issues_assets = load_assets_from_modules(modules=[pyairbyte_project])
dbt_project_assets = load_assets_from_modules(modules=[dbt])

defs = Definitions(
    assets=github_issues_assets + dbt_project_assets,
    resources={
        "database": database_resource,
        "cache": pyairbyte_cache_resource,
        "dbt": dbt_resource,
    },
    jobs=[all_asset_job, pyairbyte_asset_job, dbt_dep_job]
)
