WITH markup AS (
    SELECT
    *,
    first_value(customer_id)
    over(partition by company_name, contact_name 
    order by company_name 
    rows between unbounded preceding and unbounded following) as result
    FROM {{ source('sources', 'customers') }}
    ), 
    removed AS (
        SELECT 
        distinct result 
        FROM markup
    ),

    final AS (
     SELECT * FROM {{ source('sources', 'customers') }} WHERE customer_id in (select result from removed)
 )

  SELECT * FROM final