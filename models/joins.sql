WITH prod as (SELECT
ct.category_name,
sp.company_name as suppliers,
pd.product_name,
pd.unit_price,
pd.product_id
FROM {{ source('sources', 'products') }} AS pd
LEFT JOIN {{ source('sources', 'suppliers') }} AS sp ON pd.supplier_id = sp.supplier_id
LEFT JOIN {{ source('sources', 'categories') }} AS ct ON pd.category_id = ct.category_id
),

orddetai as (
    SELECT 
    pd.*,
    od.order_id,
    od.quantity,
    od.discount
    FROM {{ ref('orderdetails') }} AS od
    LEFT JOIN prod as pd on od.product_id = pd.product_id 
),

ordrs as (
    SELECT
    ord.order_date,
    ord.order_id,
    cs.company_name as customer,
    em.name as employee,
    em.age,
    em.lenghtofservice
    FROM {{ source('sources', 'orders') }} AS ord
    LEFT JOIN {{ ref('customers') }} AS cs ON ord.customer_id = cs.customer_id
    LEFT JOIN {{ ref('employees') }} AS em ON ord.employee_id = em.employee_id
    LEFT JOIN {{ source('sources', 'shippers') }} AS sh ON ord.ship_via = sh.shipper_id
),

finaljoin as (
    SELECT
    od.*,
    ord.order_date,
    ord.customer,
    ord.employee,
    ord.age,
    ord.lenghtofservice
    FROM orddetai od 
    INNER JOIN ordrs ord on od.order_id = ord.order_id
)

SELECT * FROM finaljoin