query = """

INSERT INTO fact_transactions (transaction_id, transaction_timestamp, entity_id, amount, tx_status, transaction_type)
SELECT
    sd.id AS transaction_id,
    sd.event_timestamp AS transaction_timestamp,
    sd.user_id AS entity_id,
    sd.amount,
    sd.tx_status,
    'deposit' AS transaction_type
FROM
    silver_deposit sd

UNION ALL

SELECT
    sw.id AS transaction_id,
    sw.event_timestamp AS transaction_timestamp,
    user_id as entity_id,
    sw.amount,
    sw.tx_status,
    'withdrawal' AS transaction_type
FROM
    silver_withdrawals sw

"""