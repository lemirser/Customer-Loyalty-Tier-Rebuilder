CREATE TABLE IF NOT EXISTS pyspark_tut.loyalty_program.customer_tier (
    record_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id INT,
    total_transaction FLOAT,
    transaction_count INT,
    tier STRING,
    previous_tier STRING,
    update_date TIMESTAMP
);