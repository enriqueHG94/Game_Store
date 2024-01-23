-- Which products have a low stock alert in each store?

SELECT
    s.store_name,
    p.product_name,
    i.available_stock
FROM
    csv.fct_inventory AS i
JOIN
    csv.dim_stores AS s ON i.store_id = s.store_id
JOIN
    csv.dim_products AS p ON i.product_id = p.product_id
WHERE
    i.low_stock_alert = TRUE;

-- What is the total available stock for each product across all stores?

SELECT
    p.product_name,
    SUM(i.available_stock) AS total_stock
FROM
    csv.fct_inventory AS i
JOIN
    csv.dim_products AS p ON i.product_id = p.product_id
GROUP BY
    p.product_name;

-- Which stores have the highest number of products with low stock?

SELECT
    s.store_name,
    COUNT(*) AS num_products_low_stock
FROM
    csv.fct_inventory AS i
JOIN
    csv.dim_stores AS s ON i.store_id = s.store_id
WHERE
    i.low_stock_alert = TRUE
GROUP BY
    s.store_name
ORDER BY
    num_products_low_stock DESC;


-- How does the available stock of a specific product vary among stores?
-- For this query, you will need to specify a product_name.

SELECT
    s.store_name,
    p.product_name,
    i.available_stock
FROM
    csv.fct_inventory AS i
JOIN
    csv.dim_stores AS s ON i.store_id = s.store_id
JOIN
    csv.dim_products AS p ON i.product_id = p.product_id
WHERE
    p.product_name = 'The Legend of Zelda: Breath of the Wild'; -- Replace with the specific product name

-- Which product genres most frequently have low stock alerts?

SELECT
    p.genre,
    COUNT(*) AS frecuency_of_low_stock_alerts
FROM
    csv.fct_inventory AS i
JOIN
    csv.dim_products AS p ON i.product_id = p.product_id
WHERE
    i.low_stock_alert = TRUE
GROUP BY
    p.genre
ORDER BY
    frecuency_of_low_stock_alerts DESC;