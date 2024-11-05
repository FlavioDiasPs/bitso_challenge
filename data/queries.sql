--##### SQLITE QUERIES #####

-- How many users were active on a given day (they made a deposit or withdrawal)
SELECT COUNT(DISTINCT entity_id) AS active_users
FROM fact_transactions
WHERE date(fact_transactions.transaction_timestamp) = '2020-01-02'

-- Identify users haven't made a deposit
SELECT user_id
FROM silver_user_id
WHERE user_id NOT IN (
    SELECT DISTINCT entity_id
    FROM fact_transactions
    WHERE transaction_type = 'deposit'
);

-- Average deposited amount for Level 2 users in the mx jurisdiction
SELECT AVG(amount) AS average_deposit
FROM fact_transactions
JOIN dim_entity ON fact_transactions.entity_id = dim_entity.entity_id
WHERE dim_entity.user_level = 2 
AND dim_entity.jurisdiction = 'mx' 
AND transaction_type = 'deposit';

-- Latest user level for each user within each jurisdiction 
----Is it expected to have is_current = 1 for each user and jurisdiction?
---- this way we could change scd 2 on dim_entity
SELECT user_id, jurisdiction, MAX(event_timestamp) AS latest_event
FROM silver_user_level
GROUP BY user_id, jurisdiction;

-- Identify on a given day which users have made more than 5 deposits historically
SELECT entity_id, transaction_timestamp
FROM fact_transactions
WHERE date(transaction_timestamp) == '2021-02-16' and transaction_type = 'deposit'
GROUP BY entity_id
HAVING COUNT(transaction_id) > 5

-- When was the last time a user made a login
SELECT user_id, MAX(event_timestamp) AS last_login
FROM silver_event
WHERE user_id = 'bd47dc58209bc820d555f935bf055e40' and event_name = 'login';

-- How many times a user has made a login between two dates
SELECT COUNT(*) AS login_count
FROM silver_event
WHERE user_id = 'bd47dc58209bc820d555f935bf055e40' 
AND event_name = 'login' 
AND event_timestamp BETWEEN '2023-08-20 20:28:39.765000' AND '2023-08-30 20:28:39.765000';

-- Number of unique currencies deposited on a given day
SELECT COUNT(DISTINCT currency) AS unique_currencies_deposited
FROM fact_transactions ft
JOIN silver_deposit sd on ft.transaction_id = sd.id
WHERE date(transaction_timestamp) = '2020-06-20' 
AND transaction_type = 'deposit';

-- Number of unique currencies withdrew on a given day
SELECT COUNT(DISTINCT currency) AS unique_currencies_deposited
FROM fact_transactions ft
JOIN silver_withdrawals sd on ft.transaction_id = sd.id
WHERE date(transaction_timestamp) = '2020-06-20' 
AND transaction_type = 'withdrawal';


-- Total amount deposited of a given currency on a given day
SELECT SUM(ft.amount) AS total_deposited
FROM fact_transactions ft
JOIN silver_deposit sd ON ft.transaction_id = sd.id
WHERE date(transaction_timestamp) = '2020-06-20' 
AND ft.transaction_type = 'deposit' 
AND sd.currency = 'mxn';
