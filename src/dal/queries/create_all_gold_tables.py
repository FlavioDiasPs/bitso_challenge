query = """
            CREATE TABLE dim_entity (
                entity_id STRING,
                entity_type STRING,
                jurisdiction STRING,
                user_level INTEGER,
                event_timestamp TIMESTAMP,
                start_date DATE,
                end_date DATE,
                is_current BOOLEAN,
                PRIMARY KEY (entity_id, event_timestamp)
            );


            CREATE TABLE dim_date (
                full_date DATE PRIMARY KEY,
                day_of_week STRING,
                month INTEGER,
                quarter INTEGER,
                year INTEGER
            );

            -- Create fact tables
            CREATE TABLE fact_transactions (
                transaction_id INTEGER,
                transaction_timestamp TIMESTAMP,
                entity_id STRING,
                amount FLOAT,
                tx_status STRING,
                transaction_type STRING 
            );


"""
