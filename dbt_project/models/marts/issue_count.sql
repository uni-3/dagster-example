{{ config(
    tags=["github_issues"]
) }}

with commits as (
    select 
      created_at
    from {{ source('raw_data', 'issues') }}
)
select 
    date_trunc('day', created_at::timestamp) as day,
    count(*) as issue_count
from commits
group by day
order by day
