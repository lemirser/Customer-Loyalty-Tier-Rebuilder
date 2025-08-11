-- Table to anonymize the customer information
CREATE TABLE customer_anon AS (
    SELECT id,
        CONCAT('fname', id) AS fname,
        CONCAT('lname', id) AS lname,
        CONCAT(
            'fname_lname_',
            id,
            '@',
            SUBSTRING_INDEX(email, '@', -1)
        ) AS email,
        registration_date
    FROM customers
);