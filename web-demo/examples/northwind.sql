-- Northwind Database Example
-- Demonstrates SQL:1999 features including:
-- - CREATE TABLE with constraints
-- - Foreign key relationships
-- - INSERT statements
-- - Complex queries with JOINs, aggregates, and ORDER BY

-- Categories table
CREATE TABLE categories (
    category_id INTEGER PRIMARY KEY,
    category_name VARCHAR(50) NOT NULL,
    description VARCHAR(255)
);

INSERT INTO categories (category_id, category_name, description) VALUES
(1, 'Beverages', 'Soft drinks, coffees, teas, beers, and ales'),
(2, 'Condiments', 'Sweet and savory sauces, relishes, spreads, and seasonings'),
(3, 'Confections', 'Desserts, candies, and sweet breads'),
(4, 'Dairy Products', 'Cheeses'),
(5, 'Grains/Cereals', 'Breads, crackers, pasta, and cereal'),
(6, 'Meat/Poultry', 'Prepared meats'),
(7, 'Produce', 'Dried fruit and bean curd'),
(8, 'Seafood', 'Seaweed and fish');

-- Products table
CREATE TABLE products (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    category_id INTEGER,
    unit_price DECIMAL(10, 2),
    FOREIGN KEY (category_id) REFERENCES categories(category_id)
);

INSERT INTO products (product_id, product_name, category_id, unit_price) VALUES
(1, 'Chai', 1, 18.00),
(2, 'Chang', 1, 19.00),
(3, 'Aniseed Syrup', 2, 10.00),
(4, 'Chef Antons Cajun Seasoning', 2, 22.00),
(5, 'Chef Antons Gumbo Mix', 2, 21.35),
(6, 'Grandmas Boysenberry Spread', 2, 25.00),
(7, 'Uncle Bobs Organic Dried Pears', 7, 30.00),
(8, 'Northwoods Cranberry Sauce', 2, 40.00),
(9, 'Mishi Kobe Niku', 6, 97.00),
(10, 'Ikura', 8, 31.00),
(11, 'Queso Cabrales', 4, 21.00),
(12, 'Queso Manchego La Pastora', 4, 38.00),
(13, 'Konbu', 8, 6.00),
(14, 'Tofu', 7, 23.25),
(15, 'Genen Shouyu', 2, 15.50),
(16, 'Pavlova', 3, 17.45),
(17, 'Alice Mutton', 6, 39.00),
(18, 'Carnarvon Tigers', 8, 62.50),
(19, 'Teatime Chocolate Biscuits', 3, 9.20),
(20, 'Sir Rodneys Marmalade', 3, 81.00),
(21, 'Sir Rodneys Scones', 3, 10.00),
(22, 'Gustafs Knäckebröd', 5, 21.00),
(23, 'Tunnbröd', 5, 9.00),
(24, 'Guaraná Fantástica', 1, 4.50),
(25, 'NuNuCa Nuß-Nougat-Creme', 3, 14.00);

-- Employees table
CREATE TABLE employees (
    employee_id INTEGER PRIMARY KEY,
    last_name VARCHAR(50) NOT NULL,
    first_name VARCHAR(50) NOT NULL,
    title VARCHAR(50),
    reports_to INTEGER,
    FOREIGN KEY (reports_to) REFERENCES employees(employee_id)
);

INSERT INTO employees (employee_id, last_name, first_name, title, reports_to) VALUES
(1, 'Davolio', 'Nancy', 'Sales Representative', 2),
(2, 'Fuller', 'Andrew', 'Vice President, Sales', NULL),
(3, 'Leverling', 'Janet', 'Sales Representative', 2),
(4, 'Peacock', 'Margaret', 'Sales Representative', 2),
(5, 'Buchanan', 'Steven', 'Sales Manager', 2),
(6, 'Suyama', 'Michael', 'Sales Representative', 5),
(7, 'King', 'Robert', 'Sales Representative', 5),
(8, 'Callahan', 'Laura', 'Inside Sales Coordinator', 2),
(9, 'Dodsworth', 'Anne', 'Sales Representative', 5);

-- Customers table
CREATE TABLE customers (
    customer_id VARCHAR(10) PRIMARY KEY,
    company_name VARCHAR(100) NOT NULL,
    contact_name VARCHAR(50),
    country VARCHAR(50)
);

INSERT INTO customers (customer_id, company_name, contact_name, country) VALUES
('ALFKI', 'Alfreds Futterkiste', 'Maria Anders', 'Germany'),
('ANATR', 'Ana Trujillo Emparedados y helados', 'Ana Trujillo', 'Mexico'),
('ANTON', 'Antonio Moreno Taquería', 'Antonio Moreno', 'Mexico'),
('AROUT', 'Around the Horn', 'Thomas Hardy', 'UK'),
('BERGS', 'Berglunds snabbköp', 'Christina Berglund', 'Sweden'),
('BLAUS', 'Blauer See Delikatessen', 'Hanna Moos', 'Germany'),
('BLONP', 'Blondel père et fils', 'Frédérique Citeaux', 'France'),
('BOLID', 'Bólido Comidas preparadas', 'Martín Sommer', 'Spain'),
('BONAP', 'Bon app', 'Laurence Lebihan', 'France'),
('BOTTM', 'Bottom-Dollar Markets', 'Elizabeth Lincoln', 'Canada'),
('BSBEV', 'Bs Beverages', 'Victoria Ashworth', 'UK'),
('CACTU', 'Cactus Comidas para llevar', 'Patricio Simpson', 'Argentina'),
('CENTC', 'Centro comercial Moctezuma', 'Francisco Chang', 'Mexico'),
('CHOPS', 'Chop-suey Chinese', 'Yang Wang', 'Switzerland'),
('COMMI', 'Comércio Mineiro', 'Pedro Afonso', 'Brazil'),
('CONSH', 'Consolidated Holdings', 'Elizabeth Brown', 'UK'),
('DRACD', 'Drachenblut Delikatessen', 'Sven Ottlieb', 'Germany'),
('DUMON', 'Du monde entier', 'Janine Labrune', 'France'),
('EASTC', 'Eastern Connection', 'Ann Devon', 'UK'),
('ERNSH', 'Ernst Handel', 'Roland Mendel', 'Austria');

-- Orders table
CREATE TABLE orders (
    order_id INTEGER PRIMARY KEY,
    customer_id VARCHAR(10),
    employee_id INTEGER,
    order_date DATE,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (employee_id) REFERENCES employees(employee_id)
);

INSERT INTO orders (order_id, customer_id, employee_id, order_date) VALUES
(10248, 'ALFKI', 5, '1996-07-04'),
(10249, 'ANATR', 6, '1996-07-05'),
(10250, 'AROUT', 4, '1996-07-08'),
(10251, 'ALFKI', 3, '1996-07-08'),
(10252, 'BERGS', 4, '1996-07-09'),
(10253, 'BLAUS', 3, '1996-07-10'),
(10254, 'BLONP', 5, '1996-07-11'),
(10255, 'BOLID', 9, '1996-07-12'),
(10256, 'BONAP', 3, '1996-07-15'),
(10257, 'BOTTM', 4, '1996-07-16'),
(10258, 'BSBEV', 1, '1996-07-17'),
(10259, 'CACTU', 8, '1996-07-18'),
(10260, 'CENTC', 4, '1996-07-19'),
(10261, 'CHOPS', 4, '1996-07-19'),
(10262, 'COMMI', 8, '1996-07-22'),
(10263, 'CONSH', 9, '1996-07-23'),
(10264, 'DRACD', 6, '1996-07-24'),
(10265, 'DUMON', 2, '1996-07-25'),
(10266, 'EASTC', 3, '1996-07-26'),
(10267, 'ERNSH', 4, '1996-07-29');

-- Order_Details table
CREATE TABLE order_details (
    order_id INTEGER,
    product_id INTEGER,
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    PRIMARY KEY (order_id, product_id),
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

INSERT INTO order_details (order_id, product_id, quantity, unit_price) VALUES
(10248, 11, 12, 14.00),
(10248, 1, 10, 18.00),
(10249, 14, 9, 23.25),
(10249, 2, 40, 19.00),
(10250, 3, 10, 10.00),
(10250, 5, 35, 21.35),
(10251, 22, 6, 21.00),
(10251, 4, 15, 22.00),
(10252, 6, 40, 25.00),
(10252, 20, 25, 81.00),
(10253, 11, 20, 21.00),
(10253, 12, 15, 38.00),
(10254, 7, 30, 30.00),
(10254, 16, 35, 17.45),
(10255, 2, 20, 19.00),
(10255, 9, 40, 97.00),
(10256, 13, 10, 6.00),
(10256, 17, 35, 39.00),
(10257, 18, 15, 62.50),
(10257, 10, 25, 31.00),
(10258, 8, 15, 40.00),
(10258, 24, 6, 4.50),
(10259, 19, 18, 9.20),
(10259, 21, 20, 10.00),
(10260, 23, 15, 9.00),
(10260, 25, 35, 14.00),
(10261, 1, 20, 18.00),
(10261, 3, 25, 10.00),
(10262, 5, 12, 21.35),
(10262, 7, 15, 30.00),
(10263, 9, 20, 97.00),
(10263, 11, 70, 21.00),
(10264, 13, 3, 6.00),
(10264, 14, 25, 23.25),
(10265, 15, 8, 15.50),
(10265, 16, 15, 17.45),
(10266, 17, 35, 39.00),
(10266, 18, 15, 62.50),
(10267, 19, 30, 9.20),
(10267, 20, 40, 81.00);

-- Example Queries
-- (These are comments showing how to use the database)

-- Basic SELECT with WHERE
-- SELECT * FROM products WHERE category_id = 1;

-- INNER JOIN across multiple tables
-- SELECT o.order_id, c.company_name, e.first_name || ' ' || e.last_name AS employee_name, o.order_date
-- FROM orders o
-- JOIN customers c ON o.customer_id = c.customer_id
-- JOIN employees e ON o.employee_id = e.employee_id
-- ORDER BY o.order_date;

-- Aggregates with GROUP BY
-- SELECT c.category_name, COUNT(p.product_id) AS product_count, AVG(p.unit_price) AS avg_price
-- FROM categories c
-- LEFT JOIN products p ON c.category_id = p.category_id
-- GROUP BY c.category_id, c.category_name
-- ORDER BY product_count DESC;

-- Complex query with multiple JOINs and aggregation
-- SELECT c.company_name, COUNT(o.order_id) AS order_count, SUM(od.quantity * od.unit_price) AS total_revenue
-- FROM customers c
-- JOIN orders o ON c.customer_id = o.customer_id
-- JOIN order_details od ON o.order_id = od.order_id
-- GROUP BY c.customer_id, c.company_name
-- ORDER BY total_revenue DESC
-- LIMIT 10;
