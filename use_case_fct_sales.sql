-- Total sales amount by store?

SELECT
    s.store_name,
    SUM(fs.total_price) AS "TOTAL_SALES"
FROM
    csv.fct_sales AS fs
JOIN
    csv.dim_stores AS s ON fs.store_id = s.store_id
GROUP BY
    s.store_name;

-- Most sold products in each region?

SELECT
    s.region,
    p.product_name,
    SUM(fs.quantity) AS "QUANTITY_SOLD"
FROM
    csv.fct_sales AS fs
JOIN
    csv.dim_stores AS s ON fs.store_id = s.store_id
JOIN
    csv.dim_products AS p ON fs.product_id = p.product_id
GROUP BY
    s.region, p.product_name
ORDER BY
    s.region, "QUANTITY_SOLD" DESC;


-- Most used payment method by customers?

SELECT
    pm.method_name,
    COUNT(*) AS "USAGE_COUNT"
FROM
    csv.fct_sales AS fs
JOIN
    csv.dim_payment_methods AS pm ON fs.payment_method_id = pm.payment_method_id
GROUP BY
    pm.method_name
ORDER BY
    "USAGE_COUNT" DESC;

-- Employees with the highest sales in each store?

SELECT
    s.store_name,
    e.employee_name,
    SUM(fs.total_price) AS "TOTAL_SALES"
FROM
    csv.fct_sales AS fs
JOIN
    csv.dim_stores AS s ON fs.store_id = s.store_id
JOIN
    csv.dim_employees AS e ON fs.employee_id = e.employee_id
GROUP BY
    s.store_name, e.employee_name
ORDER BY
    s.store_name, "TOTAL_SALES" DESC;

-- Influence of promotions on sales volume?

SELECT
    s.store_name,
    p.promotion_name,
    SUM(fs.quantity) AS "QUANTITY_SOLD",
    SUM(fs.total_price) AS "TOTAL_SALES"
FROM
    csv.fct_sales AS fs
JOIN
    csv.dim_stores AS s ON fs.store_id = s.store_id
JOIN
    csv.dim_promotions AS p ON fs.promotion_id = p.promotion_id
GROUP BY
    s.store_name, p.promotion_name
ORDER BY
    s.store_name, "TOTAL_SALES" DESC;