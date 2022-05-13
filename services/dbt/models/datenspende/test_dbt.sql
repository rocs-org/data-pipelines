
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

with test_dbt as (

    select *
    from datenspende.vitaldata
    limit 10

)

select *
from test_dbt

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
