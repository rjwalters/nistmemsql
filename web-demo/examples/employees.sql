-- Employees Hierarchy Database
-- Demonstrates SQL:1999 recursive queries using organizational hierarchy
-- 4 levels deep, 35 employees total

CREATE TABLE employees (
    employee_id INTEGER PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    manager_id INTEGER,  -- Self-referencing FK, NULL for CEO
    salary DECIMAL(10, 2) NOT NULL,
    department VARCHAR(50) NOT NULL,
    hire_date DATE,
    title VARCHAR(100)
);

-- Level 1: CEO (1 person)
INSERT INTO employees VALUES
  (1, 'Sarah', 'Chen', NULL, 320000.00, 'Executive', '2015-01-15', 'Chief Executive Officer');

-- Level 2: VPs (3 people)
INSERT INTO employees VALUES
  (2, 'Michael', 'Rodriguez', 1, 200000.00, 'Engineering', '2016-03-20', 'VP of Engineering'),
  (3, 'Emily', 'Johnson', 1, 190000.00, 'Sales', '2016-06-10', 'VP of Sales'),
  (4, 'David', 'Kim', 1, 185000.00, 'Operations', '2017-01-05', 'VP of Operations');

-- Level 3: Directors (9 people - 3 per VP)

-- Engineering Directors
INSERT INTO employees VALUES
  (5, 'Jennifer', 'Martinez', 2, 145000.00, 'Engineering', '2017-08-15', 'Director of Backend'),
  (6, 'Robert', 'Taylor', 2, 140000.00, 'Engineering', '2018-02-20', 'Director of Frontend'),
  (7, 'Lisa', 'Anderson', 2, 135000.00, 'Engineering', '2018-05-10', 'Director of DevOps');

-- Sales Directors
INSERT INTO employees VALUES
  (8, 'James', 'Wilson', 3, 138000.00, 'Sales', '2017-11-12', 'Director of Enterprise Sales'),
  (9, 'Maria', 'Garcia', 3, 142000.00, 'Sales', '2018-01-20', 'Director of SMB Sales'),
  (10, 'Christopher', 'Brown', 3, 135000.00, 'Sales', '2018-04-05', 'Director of Sales Operations');

-- Operations Directors
INSERT INTO employees VALUES
  (11, 'Amanda', 'Lee', 4, 130000.00, 'Operations', '2018-07-15', 'Director of Customer Success'),
  (12, 'Daniel', 'Moore', 4, 128000.00, 'Operations', '2018-09-01', 'Director of Finance'),
  (13, 'Jessica', 'Davis', 4, 132000.00, 'Operations', '2019-01-10', 'Director of HR');

-- Level 4: Individual Contributors (22 people)

-- Backend Team (4 people under Jennifer)
INSERT INTO employees VALUES
  (14, 'Ryan', 'Thomas', 5, 95000.00, 'Engineering', '2019-03-15', 'Senior Backend Engineer'),
  (15, 'Nicole', 'Jackson', 5, 88000.00, 'Engineering', '2019-07-20', 'Backend Engineer'),
  (16, 'Kevin', 'White', 5, 92000.00, 'Engineering', '2020-01-10', 'Senior Backend Engineer'),
  (17, 'Laura', 'Harris', 5, 85000.00, 'Engineering', '2020-06-15', 'Backend Engineer');

-- Frontend Team (4 people under Robert)
INSERT INTO employees VALUES
  (18, 'Brandon', 'Martin', 6, 90000.00, 'Engineering', '2019-05-12', 'Senior Frontend Engineer'),
  (19, 'Michelle', 'Thompson', 6, 83000.00, 'Engineering', '2019-09-18', 'Frontend Engineer'),
  (20, 'Jason', 'Garcia', 6, 87000.00, 'Engineering', '2020-02-20', 'Frontend Engineer'),
  (21, 'Rachel', 'Martinez', 6, 91000.00, 'Engineering', '2020-08-05', 'Senior Frontend Engineer');

-- DevOps Team (3 people under Lisa)
INSERT INTO employees VALUES
  (22, 'Eric', 'Robinson', 7, 94000.00, 'Engineering', '2019-04-10', 'Senior DevOps Engineer'),
  (23, 'Stephanie', 'Clark', 7, 86000.00, 'Engineering', '2019-10-25', 'DevOps Engineer'),
  (24, 'Andrew', 'Rodriguez', 7, 89000.00, 'Engineering', '2020-03-15', 'DevOps Engineer');

-- Enterprise Sales Team (3 people under James)
INSERT INTO employees VALUES
  (25, 'Matthew', 'Lewis', 8, 82000.00, 'Sales', '2019-02-15', 'Enterprise Account Executive'),
  (26, 'Ashley', 'Lee', 8, 85000.00, 'Sales', '2019-06-20', 'Senior Enterprise AE'),
  (27, 'Joshua', 'Walker', 8, 80000.00, 'Sales', '2020-01-12', 'Enterprise Account Executive');

-- SMB Sales Team (3 people under Maria)
INSERT INTO employees VALUES
  (28, 'Sarah', 'Hall', 9, 75000.00, 'Sales', '2019-03-10', 'SMB Account Executive'),
  (29, 'David', 'Allen', 9, 78000.00, 'Sales', '2019-08-15', 'Senior SMB AE'),
  (30, 'Jennifer', 'Young', 9, 73000.00, 'Sales', '2020-02-05', 'SMB Account Executive');

-- Sales Operations Team (2 people under Christopher)
INSERT INTO employees VALUES
  (31, 'Brian', 'Hernandez', 10, 77000.00, 'Sales', '2019-05-20', 'Sales Operations Analyst'),
  (32, 'Melissa', 'King', 10, 80000.00, 'Sales', '2019-11-10', 'Senior Sales Ops Analyst');

-- Customer Success Team (2 people under Amanda)
INSERT INTO employees VALUES
  (33, 'Justin', 'Wright', 11, 72000.00, 'Operations', '2019-07-01', 'Customer Success Manager'),
  (34, 'Lauren', 'Lopez', 11, 75000.00, 'Operations', '2020-01-20', 'Senior CSM');

-- Finance Team (1 person under Daniel)
INSERT INTO employees VALUES
  (35, 'Tyler', 'Hill', 12, 70000.00, 'Operations', '2020-04-15', 'Financial Analyst');
