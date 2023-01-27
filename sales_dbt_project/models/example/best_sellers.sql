
-- Use the `ref` function to select from other models

{{ config(materialized='table') }}

select *
from {{ ref('sales_revenue_by_product') }}
order by total_quantity DESC
