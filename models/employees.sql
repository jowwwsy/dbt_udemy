WITH calc_employees as (
    SELECT 
    *,
date_part(year, current_date) - date_part(year, birth_date) as age,
date_part(year, current_date) - date_part(year, hire_date) as lenghtofservice,
first_name || ' ' || last_name as name
FROM {{ source('sources', 'employees') }}
)

SELECT * FROM calc_employees