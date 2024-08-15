from dagster import asset, OpExecutionContext
import airbyte as ab
import pandas as pd


@asset(
    name="issues",
    group_name="airbyte_sources",
    compute_kind="DuckDB",
    required_resource_keys={'cache', 'database'},
)
def github_issues(context: OpExecutionContext):
    repo_name = "airbytehq/quickstarts"
    stream_name = "issues"
    cache = context.resources.cache
    db = context.resources.database
    result = load_gihtub_issues(repo_name, stream_name, cache)

    table_name = "issues"
    schema_name = "staging"
    with db.get_connection() as conn:
        # to duckdbはschema指定が有効でない
        # result.to_sql(table_name, conn, schema="source", if_exists='append', index=False)
        # or
        conn.execute(f"CREATE TABLE {schema_name}.{table_name} AS SELECT * FROM result")

    count = len(result)
    res = {"table_name": table_name, "row_count": count}
    return res


def load_gihtub_issues(repo_name: str, stream_name: str, cache) -> pd.DataFrame:
    # load from github
    source = ab.get_source(
        "source-github",
        install_if_missing=True,
        config={
            "repositories": [repo_name],
            "credentials": {
                "personal_access_token": ab.get_secret("GITHUB_PERSONAL_ACCESS_TOKEN"),
            },
        },
    )

    try:
        source.check()
    except Exception as e:
        print(f"Error source check: {str(e)}")

    source.select_streams([stream_name])

    result = source.read(cache=cache)
    return result.cache[stream_name].to_pandas()
