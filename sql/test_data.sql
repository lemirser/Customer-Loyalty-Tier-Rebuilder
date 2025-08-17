-- | Tier | Total Spent | Transaction Count |
-- |------|-------------|-------------------|
-- | **Platinum** | ≥ ₱100,000 | ≥ 20 |
-- | **Gold** | ≥ ₱50,000 | ≥ 10 |
-- | **Silver** | ≥ ₱20,000 | ≥ 5 |
-- | **Bronze** | < ₱20,000 | < 5 |
-- Customer tier process
-- 1. Cleanup critical tables
-- TRUNCATE TABLE transactions;
-- TRUNCATE TABLE customer_anon;
-- DELETE FROM transactions;
-- DELETE FROM customer_anon;
--2. Create a customer
INSERT INTO customer_anon (
        fname,
        lname,
        email,
        registration_date
    )
VALUES ('John', 'Doe', 'johndoe@gmail.com', '2022-01-01'),
    ('Jane', 'Doe', 'janedoe@gmail.com', '2022-01-01');
-- 3. Create transactions (make sure that transaction dates are within a year from current date)
-- Bronze Tier | < ₱20,000 | < 5
INSERT INTO transactions (customer_id, transaction_date, amount)
VALUES (1, '2024-09-01', 500.32),
    (2, '2024-09-01', 2894.51),
    (2, '2024-09-01', NULL),
    (2, '2024-09-01', 11234.51),
    (1, '2024-09-02', 1238.44),
    (1, '2024-09-02', NULL),
    (1, '2024-09-02', NULL),
    (1, '2024-09-03', 0),
    (1, '2024-09-04', 0);
-- Silver Tier | ≥ ₱20,000 | ≥ 5
INSERT INTO transactions (customer_id, transaction_date, amount)
VALUES (1, '2024-09-05', 10432.55),
    (2, '2024-09-06', 500.00),
    (2, '2024-09-06', 6923.50);
-- Gold Tier | ≥ ₱50,000 | ≥ 10
INSERT INTO transactions (customer_id, transaction_date, amount)
VALUES (2, '2024-09-10', 3592.00),
    (2, '2024-09-10', NULL),
    (1, '2024-09-11', 10432.55),
    (1, '2024-09-11', 583.9),
    (2, '2024-09-15', 6923.50),
    (1, '2024-09-16', 2489.03),
    (1, '2024-09-16', NULL),
    (1, '2024-09-16', 3982.89),
    (2, '2024-09-18', 8948.40),
    (2, '2024-09-25', 11197.57);
-- Platinum Tier | ≥ ₱100,000 | ≥ 20
INSERT INTO transactions (customer_id, transaction_date, amount)
VALUES (2, '2024-10-04', 3592.00),
    (2, '2024-10-04', NULL),
    (2, '2024-10-04', NULL),
    (1, '2024-10-16', NULL),
    (2, '2024-10-16', 5921.32),
    (1, '2024-10-16', NULL),
    (1, '2024-10-16', 3029.49),
    (1, '2024-10-20', 3531.93),
    (1, '2024-10-20', 4392.10),
    (1, '2024-10-21', 2951.02),
    (2, '2024-10-21', 4312.94),
    (2, '2024-10-21', 2222.32),
    (1, '2024-10-22', 5912.09),
    (1, '2024-10-25', 1245.64),
    (2, '2024-10-25', 5312.32),
    (1, '2024-10-26', 5312),
    (1, '2024-10-30', 3531.93),
    (1, '2024-10-30', NULL),
    (2, '2024-10-30', 3921.59),
    (1, '2024-10-30', NULL),
    (1, '2024-10-30', 4215.63),
    (2, '2024-10-30', 3221.98),
    (2, '2024-11-04', 1123.43),
    (2, '2024-11-04', 5921.32);