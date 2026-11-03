DROP TABLE IF EXISTS sales;

CREATE TABLE sales AS
SELECT DISTINCT
    order_date,
    customer_name,
    product,
    quantity,
    price,
    COALESCE(region, 'Unknown') AS region,
    quantity * price AS total_sale
FROM raw_sales
WHERE quantity IS NOT NULL
  AND price IS NOT NULL;
