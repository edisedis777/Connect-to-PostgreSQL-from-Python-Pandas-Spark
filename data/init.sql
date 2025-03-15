-- Create a sample table for our demonstrations
CREATE TABLE IF NOT EXISTS sales (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    product_id INTEGER NOT NULL,
    product_name VARCHAR(100) NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    customer_id INTEGER,
    region VARCHAR(50)
);

-- Insert some sample data
INSERT INTO sales (date, product_id, product_name, quantity, unit_price, customer_id, region)
VALUES
    ('2023-01-15', 1, 'Laptop', 5, 1200.00, 101, 'North'),
    ('2023-01-16', 2, 'Mouse', 10, 25.50, 102, 'South'),
    ('2023-01-17', 3, 'Keyboard', 7, 45.99, 103, 'East'),
    ('2023-01-18', 1, 'Laptop', 3, 1200.00, 104, 'West'),
    ('2023-01-19', 4, 'Monitor', 6, 250.00, 105, 'North'),
    ('2023-01-20', 2, 'Mouse', 15, 25.50, 106, 'South'),
    ('2023-01-21', 5, 'Headphones', 12, 75.00, 107, 'East'),
    ('2023-01-22', 3, 'Keyboard', 8, 45.99, 108, 'West'),
    ('2023-01-23', 4, 'Monitor', 4, 250.00, 109, 'North'),
    ('2023-01-24', 5, 'Headphones', 9, 75.00, 110, 'South');

-- Create a users table for join examples
CREATE TABLE IF NOT EXISTS customers (
    customer_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL,
    signup_date DATE NOT NULL
);

-- Insert sample customer data
INSERT INTO customers (customer_id, name, email, signup_date)
VALUES
    (101, 'John Smith', 'john.smith@example.com', '2022-10-01'),
    (102, 'Jane Doe', 'jane.doe@example.com', '2022-10-05'),
    (103, 'Bob Johnson', 'bob.johnson@example.com', '2022-10-10'),
    (104, 'Alice Brown', 'alice.brown@example.com', '2022-10-15'),
    (105, 'Charlie Davis', 'charlie.davis@example.com', '2022-10-20'),
    (106, 'Diana Wilson', 'diana.wilson@example.com', '2022-10-25'),
    (107, 'Edward Martin', 'edward.martin@example.com', '2022-11-01'),
    (108, 'Fiona Taylor', 'fiona.taylor@example.com', '2022-11-05'),
    (109, 'George Clark', 'george.clark@example.com', '2022-11-10'),
    (110, 'Hannah Lewis', 'hannah.lewis@example.com', '2022-11-15');
