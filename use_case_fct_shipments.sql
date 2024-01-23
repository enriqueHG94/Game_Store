-- Average delivery time by shipping company?

SELECT
    fs.shipping_company,
    AVG(DATEDIFF(day, fs.shipping_date, fs.estimated_delivery_date)) AS "AVERAGE_DELIVERY_TIME"
FROM
    csv.fct_shipments AS fs
GROUP BY
    fs.shipping_company;

-- Percentage of on-time deliveries?

SELECT
    fs.shipping_company,
    COUNT(*) AS "TOTAL_SHIPMENTS",
    SUM(CASE WHEN fs.delivery_status = 'Entregado' THEN 1 ELSE 0 END) AS "ON_TIME_DELIVERIES",
    100 * SUM(CASE WHEN fs.delivery_status = 'Entregado' THEN 1 ELSE 0 END) / COUNT(*) AS "ON_TIME_DELIVERY_PERCENTAGE"
FROM
    csv.fct_shipments AS fs
GROUP BY
    fs.shipping_company;

-- Most common destinations for shipments?

SELECT
    fs.shipping_address AS "DESTINATION",
    COUNT(*) AS "NUMBER_OF_SHIPMENTS"
FROM
    csv.fct_shipments AS fs
GROUP BY
    fs.shipping_address
ORDER BY
    "NUMBER_OF_SHIPMENTS" DESC;

-- Average shipping cost by region?

SELECT
    c.region,
    AVG(fs.shipping_cost) AS "AVERAGE_SHIPPING_COST"
FROM
    csv.fct_shipments AS fs
JOIN
    csv.dim_customers AS c ON fs.customer_id = c.customer_id
GROUP BY
    c.region;

-- Relationship between shipping cost and delivery punctuality?

SELECT
    fs.shipping_company,
    AVG(fs.shipping_cost) AS "AVERAGE_SHIPPING_COST",
    100 * SUM(CASE WHEN fs.delivery_status = 'On Time' THEN 1 ELSE 0 END) / COUNT(*) AS "ON_TIME_DELIVERY_PERCENTAGE"
FROM
    csv.fct_shipments AS fs
GROUP BY
    fs.shipping_company;