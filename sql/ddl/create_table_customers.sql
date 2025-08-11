CREATE TABLE IF NOT EXISTS customers (
    `id` int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `fname` varchar(50) NOT NULL,
    `lname` varchar(50) NOT NULL,
    `email` varchar(50) NOT NULL,
    `registration_date` DATE NOT NULL
);