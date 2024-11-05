query = """

-- Step 1: Rank levels to determine the start and end dates for each user level change
CREATE TEMPORARY TABLE ranked_levels AS
SELECT
    su.user_id AS entity_id,
    sul.jurisdiction,
    sul.level AS user_level,
    sul.event_timestamp,
    sul.event_timestamp AS start_date,
    LEAD(sul.event_timestamp) OVER (PARTITION BY su.user_id ORDER BY sul.event_timestamp) AS end_date
FROM
    silver_user_id su
JOIN
    silver_user_level sul ON su.user_id = sul.user_id;

-- Step 2: Determine the current levels, setting end_date to a far future date if it's NULL
CREATE TEMPORARY TABLE current_levels AS
SELECT
    rl.entity_id,
    rl.jurisdiction,
    rl.user_level,
    rl.event_timestamp,
    rl.start_date,
    COALESCE(rl.end_date, DATE '9999-12-31') AS end_date,
    CASE WHEN rl.end_date IS NULL THEN 1 ELSE 0 END AS is_current
FROM
    ranked_levels rl;

-- Step 3: Insert all records into dim_entity
INSERT INTO dim_entity (entity_id, entity_type, jurisdiction, user_level, event_timestamp, start_date, end_date, is_current)
SELECT
    cl.entity_id,
    'human' AS entity_type,
    cl.jurisdiction,
    cl.user_level,
    cl.event_timestamp,
    cl.start_date,
    cl.end_date,
    cl.is_current
FROM
    current_levels cl;

-- Step 4: Set is_current = 1 for the latest records using a JOIN without considering jurisdiction
UPDATE dim_entity
SET is_current = 1
FROM (
    SELECT entity_id, MAX(event_timestamp) AS latest_event
    FROM dim_entity
    GROUP BY entity_id
) AS latest
WHERE dim_entity.entity_id = latest.entity_id
AND dim_entity.event_timestamp = latest.latest_event;



"""
# query = """
# -- Step 1: Rank levels to determine the start and end dates for each user level change
# CREATE TEMPORARY TABLE ranked_levels AS
# SELECT
#     su.user_id AS entity_id,
#     sul.jurisdiction,
#     sul.level AS user_level,
#     sul.event_timestamp,
#     sul.event_timestamp AS start_date,
#     LEAD(sul.event_timestamp) OVER (PARTITION BY su.user_id, sul.jurisdiction ORDER BY sul.event_timestamp) AS end_date
# FROM
#     silver_user_id su
# JOIN
#     silver_user_level sul ON su.user_id = sul.user_id;

# -- Step 2: Determine the current levels, setting end_date to a far future date if it's NULL
# CREATE TEMPORARY TABLE current_levels AS
# SELECT
#     rl.entity_id,
#     rl.jurisdiction,
#     rl.user_level,
#     rl.event_timestamp,
#     rl.start_date,
#     COALESCE(rl.end_date, DATE '9999-12-31') AS end_date,
#     CASE WHEN rl.end_date IS NULL THEN 1 ELSE 0 END AS is_current
# FROM
#     ranked_levels rl;

# -- Step 3: Update existing records in dim_entity to set the end_date and mark them as non-current
# UPDATE dim_entity
# SET end_date = cl.start_date - INTERVAL '1 day',
#     is_current = 0
# FROM current_levels cl
# WHERE dim_entity.entity_id = cl.entity_id
# AND dim_entity.jurisdiction = cl.jurisdiction
# AND dim_entity.is_current = 1
# AND cl.start_date > dim_entity.start_date;

# -- Step 4: Insert new records into dim_entity, ensuring no duplicates
# INSERT INTO dim_entity (entity_id, entity_type, jurisdiction, user_level, event_timestamp, start_date, end_date, is_current)
# SELECT
#     cl.entity_id,
#     'human' AS entity_type,
#     cl.jurisdiction,
#     cl.user_level,
#     cl.event_timestamp,
#     cl.start_date,
#     cl.end_date,
#     cl.is_current
# FROM
#     current_levels cl
# LEFT JOIN dim_entity de ON cl.entity_id = de.entity_id
#     AND cl.jurisdiction = de.jurisdiction
#     AND cl.start_date = de.start_date
#     AND cl.event_timestamp = de.event_timestamp
# WHERE de.entity_id IS NULL;

# -- Step 5: Ensure only one current record per entity_id and jurisdiction
# UPDATE dim_entity
# SET is_current = 0
# WHERE is_current = 1
# AND NOT EXISTS (
#     SELECT 1
#     FROM dim_entity lr
#     WHERE lr.entity_id = dim_entity.entity_id
#     AND lr.jurisdiction = dim_entity.jurisdiction
#     AND lr.event_timestamp = (
#         SELECT MAX(event_timestamp)
#         FROM dim_entity
#         WHERE entity_id = dim_entity.entity_id
#         AND jurisdiction = dim_entity.jurisdiction
#         AND is_current = 1
#     )
# );

# UPDATE dim_entity
# SET is_current = 1
# WHERE EXISTS (
#     SELECT 1
#     FROM dim_entity lr
#     WHERE lr.entity_id = dim_entity.entity_id
#     AND lr.jurisdiction = dim_entity.jurisdiction
#     AND lr.event_timestamp = (
#         SELECT MAX(event_timestamp)
#         FROM dim_entity
#         WHERE entity_id = dim_entity.entity_id
#         AND jurisdiction = dim_entity.jurisdiction
#         AND is_current = 1
#     )
# );

# """
