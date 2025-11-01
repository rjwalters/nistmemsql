//! Database fixtures for web demo SQL tests
//!
//! This module provides pre-configured test databases including:
//! - Northwind: Categories and products
//! - Employees: Company structure with departments, projects, and employees
//! - University: Students, courses, and enrollments
//! - Empty: Minimal empty database for testing

use catalog::{ColumnSchema, TableSchema};
use storage::{Database, Row};
use types::{DataType, SqlValue};

/// Create a Northwind database for testing
pub fn create_northwind_db() -> Database {
    let mut db = Database::new();

    // Create categories table
    let categories_schema = TableSchema::new(
        "CATEGORIES".to_string(),
        vec![
            ColumnSchema::new("CATEGORY_ID".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "CATEGORY_NAME".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new(
                "DESCRIPTION".to_string(),
                DataType::Varchar { max_length: Some(200) },
                false,
            ),
        ],
    );
    db.create_table(categories_schema).unwrap();

    // Create products table
    let products_schema = TableSchema::new(
        "PRODUCTS".to_string(),
        vec![
            ColumnSchema::new("PRODUCT_ID".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "PRODUCT_NAME".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
            ColumnSchema::new("CATEGORY_ID".to_string(), DataType::Integer, false),
            ColumnSchema::new("UNIT_PRICE".to_string(), DataType::Float { precision: 53 }, false),
            ColumnSchema::new("UNITS_IN_STOCK".to_string(), DataType::Integer, false),
            ColumnSchema::new("UNITS_ON_ORDER".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(products_schema).unwrap();

    // Insert sample data
    let categories_table = db.get_table_mut("CATEGORIES").unwrap();
    categories_table
        .insert(Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar("Beverages".to_string()),
            SqlValue::Varchar("Soft drinks, coffees, teas, beers, and ales".to_string()),
        ]))
        .unwrap();
    categories_table
        .insert(Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar("Condiments".to_string()),
            SqlValue::Varchar("Sweet and savory sauces".to_string()),
        ]))
        .unwrap();
    categories_table
        .insert(Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar("Confections".to_string()),
            SqlValue::Varchar("Desserts and candies".to_string()),
        ]))
        .unwrap();
    categories_table
        .insert(Row::new(vec![
            SqlValue::Integer(4),
            SqlValue::Varchar("Dairy Products".to_string()),
            SqlValue::Varchar("Cheeses".to_string()),
        ]))
        .unwrap();
    categories_table
        .insert(Row::new(vec![
            SqlValue::Integer(6),
            SqlValue::Varchar("Meat/Poultry".to_string()),
            SqlValue::Varchar("Prepared meats".to_string()),
        ]))
        .unwrap();
    categories_table
        .insert(Row::new(vec![
            SqlValue::Integer(7),
            SqlValue::Varchar("Produce".to_string()),
            SqlValue::Varchar("Dried fruit and bean curd".to_string()),
        ]))
        .unwrap();
    categories_table
        .insert(Row::new(vec![
            SqlValue::Integer(8),
            SqlValue::Varchar("Seafood".to_string()),
            SqlValue::Varchar("Seaweed and fish".to_string()),
        ]))
        .unwrap();

    let products_table = db.get_table_mut("PRODUCTS").unwrap();

    // Insert products - matching the expected results in examples
    products_table
        .insert(Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar("Chai".to_string()),
            SqlValue::Integer(1),
            SqlValue::Float(18.0),
            SqlValue::Integer(39),
            SqlValue::Integer(0),
        ]))
        .unwrap();

    products_table
        .insert(Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar("Chang".to_string()),
            SqlValue::Integer(1),
            SqlValue::Float(19.0),
            SqlValue::Integer(17),
            SqlValue::Integer(40),
        ]))
        .unwrap();

    products_table
        .insert(Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar("Aniseed Syrup".to_string()),
            SqlValue::Integer(2),
            SqlValue::Float(10.0),
            SqlValue::Integer(13),
            SqlValue::Integer(70),
        ]))
        .unwrap();

    products_table
        .insert(Row::new(vec![
            SqlValue::Integer(4),
            SqlValue::Varchar("Chef Anton's Cajun Seasoning".to_string()),
            SqlValue::Integer(2),
            SqlValue::Float(22.0),
            SqlValue::Integer(53),
            SqlValue::Integer(0),
        ]))
        .unwrap();

    products_table
        .insert(Row::new(vec![
            SqlValue::Integer(5),
            SqlValue::Varchar("Chef Anton's Gumbo Mix".to_string()),
            SqlValue::Integer(2),
            SqlValue::Float(21.35),
            SqlValue::Integer(0),
            SqlValue::Integer(0),
        ]))
        .unwrap();

    // Add more products for WHERE clause examples
    products_table
        .insert(Row::new(vec![
            SqlValue::Integer(6),
            SqlValue::Varchar("Mishi Kobe Niku".to_string()),
            SqlValue::Integer(3),
            SqlValue::Float(97.0),
            SqlValue::Integer(29),
            SqlValue::Integer(0),
        ]))
        .unwrap();

    products_table
        .insert(Row::new(vec![
            SqlValue::Integer(7),
            SqlValue::Varchar("Sir Rodney's Marmalade".to_string()),
            SqlValue::Integer(3),
            SqlValue::Float(81.0),
            SqlValue::Integer(40),
            SqlValue::Integer(0),
        ]))
        .unwrap();

    products_table
        .insert(Row::new(vec![
            SqlValue::Integer(8),
            SqlValue::Varchar("Carnarvon Tigers".to_string()),
            SqlValue::Integer(3),
            SqlValue::Float(62.5),
            SqlValue::Integer(42),
            SqlValue::Integer(0),
        ]))
        .unwrap();

    products_table
        .insert(Row::new(vec![
            SqlValue::Integer(9),
            SqlValue::Varchar("Northwoods Cranberry Sauce".to_string()),
            SqlValue::Integer(2),
            SqlValue::Float(40.0),
            SqlValue::Integer(6),
            SqlValue::Integer(0),
        ]))
        .unwrap();

    products_table
        .insert(Row::new(vec![
            SqlValue::Integer(10),
            SqlValue::Varchar("Alice Mutton".to_string()),
            SqlValue::Integer(3),
            SqlValue::Float(39.0),
            SqlValue::Integer(0),
            SqlValue::Integer(0),
        ]))
        .unwrap();

    products_table
        .insert(Row::new(vec![
            SqlValue::Integer(11),
            SqlValue::Varchar("Queso Manchego La Pastora".to_string()),
            SqlValue::Integer(2),
            SqlValue::Float(38.0),
            SqlValue::Integer(86),
            SqlValue::Integer(0),
        ]))
        .unwrap();

    products_table
        .insert(Row::new(vec![
            SqlValue::Integer(12),
            SqlValue::Varchar("Ikura".to_string()),
            SqlValue::Integer(3),
            SqlValue::Float(31.0),
            SqlValue::Integer(31),
            SqlValue::Integer(0),
        ]))
        .unwrap();

    products_table
        .insert(Row::new(vec![
            SqlValue::Integer(13),
            SqlValue::Varchar("Grandma's Boysenberry Spread".to_string()),
            SqlValue::Integer(2),
            SqlValue::Float(25.0),
            SqlValue::Integer(120),
            SqlValue::Integer(0),
        ]))
        .unwrap();

    products_table
        .insert(Row::new(vec![
            SqlValue::Integer(14),
            SqlValue::Varchar("Tofu".to_string()),
            SqlValue::Integer(1),
            SqlValue::Float(23.25),
            SqlValue::Integer(35),
            SqlValue::Integer(0),
        ]))
        .unwrap();

    products_table
        .insert(Row::new(vec![
            SqlValue::Integer(15),
            SqlValue::Varchar("Queso Cabrales".to_string()),
            SqlValue::Integer(2),
            SqlValue::Float(21.0),
            SqlValue::Integer(22),
            SqlValue::Integer(30),
        ]))
        .unwrap();

    // Add products for categories 4, 6, 7, 8 to ensure DISTINCT returns 7 categories
    products_table
        .insert(Row::new(vec![
            SqlValue::Integer(16),
            SqlValue::Varchar("Pavlova".to_string()),
            SqlValue::Integer(3),
            SqlValue::Float(17.45),
            SqlValue::Integer(29),
            SqlValue::Integer(0),
        ]))
        .unwrap();

    products_table
        .insert(Row::new(vec![
            SqlValue::Integer(17),
            SqlValue::Varchar("Raclette Courdavault".to_string()),
            SqlValue::Integer(4),
            SqlValue::Float(15.0),
            SqlValue::Integer(79),
            SqlValue::Integer(0),
        ]))
        .unwrap();

    products_table
        .insert(Row::new(vec![
            SqlValue::Integer(18),
            SqlValue::Varchar("Perth Pasties".to_string()),
            SqlValue::Integer(6),
            SqlValue::Float(12.8),
            SqlValue::Integer(0),
            SqlValue::Integer(0),
        ]))
        .unwrap();

    products_table
        .insert(Row::new(vec![
            SqlValue::Integer(19),
            SqlValue::Varchar("Manjimup Dried Apples".to_string()),
            SqlValue::Integer(7),
            SqlValue::Float(18.0),
            SqlValue::Integer(20),
            SqlValue::Integer(0),
        ]))
        .unwrap();

    products_table
        .insert(Row::new(vec![
            SqlValue::Integer(20),
            SqlValue::Varchar("Inlagd Sill".to_string()),
            SqlValue::Integer(8),
            SqlValue::Float(19.0),
            SqlValue::Integer(112),
            SqlValue::Integer(0),
        ]))
        .unwrap();

    db
}

/// Create an Employees database for testing
pub fn create_employees_db() -> Database {
    let mut db = Database::new();

    // Create departments table for company examples
    let departments_schema = TableSchema::new(
        "DEPARTMENTS".to_string(),
        vec![
            ColumnSchema::new("DEPT_ID".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "DEPT_NAME".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new(
                "LOCATION".to_string(),
                DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ],
    );
    db.create_table(departments_schema).unwrap();

    // Create projects table for company examples
    let projects_schema = TableSchema::new(
        "PROJECTS".to_string(),
        vec![
            ColumnSchema::new("PROJECT_ID".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "PROJECT_NAME".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
            ColumnSchema::new("DEPT_ID".to_string(), DataType::Integer, true),
            ColumnSchema::new("BUDGET".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(projects_schema).unwrap();

    // Create employees table
    let employees_schema = TableSchema::new(
        "EMPLOYEES".to_string(),
        vec![
            ColumnSchema::new("EMPLOYEE_ID".to_string(), DataType::Integer, false),
            ColumnSchema::new("EMP_ID".to_string(), DataType::Integer, false), // alias for company examples
            ColumnSchema::new(
                "FIRST_NAME".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new(
                "LAST_NAME".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new(
                "NAME".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ), // full name for company examples
            ColumnSchema::new(
                "DEPARTMENT".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new("DEPT_ID".to_string(), DataType::Integer, true), // for company examples
            ColumnSchema::new(
                "TITLE".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
            ColumnSchema::new("SALARY".to_string(), DataType::Float { precision: 53 }, false),
            ColumnSchema::new(
                "HIRE_DATE".to_string(),
                DataType::Varchar { max_length: Some(20) },
                true,
            ), // for datetime examples
            ColumnSchema::new("MANAGER_ID".to_string(), DataType::Integer, true), // nullable
        ],
    );
    db.create_table(employees_schema).unwrap();

    // Insert departments
    let departments_table = db.get_table_mut("DEPARTMENTS").unwrap();
    departments_table
        .insert(Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar("Engineering".to_string()),
            SqlValue::Varchar("San Francisco".to_string()),
        ]))
        .unwrap();
    departments_table
        .insert(Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar("Sales".to_string()),
            SqlValue::Varchar("New York".to_string()),
        ]))
        .unwrap();
    departments_table
        .insert(Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar("Marketing".to_string()),
            SqlValue::Varchar("Los Angeles".to_string()),
        ]))
        .unwrap();
    departments_table
        .insert(Row::new(vec![
            SqlValue::Integer(4),
            SqlValue::Varchar("Human Resources".to_string()),
            SqlValue::Varchar("Chicago".to_string()),
        ]))
        .unwrap();
    departments_table
        .insert(Row::new(vec![
            SqlValue::Integer(5),
            SqlValue::Varchar("Operations".to_string()),
            SqlValue::Varchar("Seattle".to_string()),
        ]))
        .unwrap();

    // Insert projects
    let projects_table = db.get_table_mut("PROJECTS").unwrap();
    projects_table
        .insert(Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar("Cloud Migration".to_string()),
            SqlValue::Integer(1),
            SqlValue::Integer(500000),
        ]))
        .unwrap();
    projects_table
        .insert(Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar("Mobile App".to_string()),
            SqlValue::Integer(1),
            SqlValue::Integer(350000),
        ]))
        .unwrap();
    projects_table
        .insert(Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar("Data Analytics Platform".to_string()),
            SqlValue::Integer(1),
            SqlValue::Integer(220000),
        ]))
        .unwrap();
    projects_table
        .insert(Row::new(vec![
            SqlValue::Integer(4),
            SqlValue::Varchar("Marketing Campaign".to_string()),
            SqlValue::Integer(3),
            SqlValue::Integer(150000),
        ]))
        .unwrap();
    projects_table
        .insert(Row::new(vec![
            SqlValue::Integer(5),
            SqlValue::Varchar("Brand Redesign".to_string()),
            SqlValue::Integer(3),
            SqlValue::Integer(125000),
        ]))
        .unwrap();
    projects_table
        .insert(Row::new(vec![
            SqlValue::Integer(6),
            SqlValue::Varchar("CRM Implementation".to_string()),
            SqlValue::Integer(2),
            SqlValue::Integer(150000),
        ]))
        .unwrap();
    projects_table
        .insert(Row::new(vec![
            SqlValue::Integer(7),
            SqlValue::Varchar("HR System Upgrade".to_string()),
            SqlValue::Integer(4),
            SqlValue::Integer(75000),
        ]))
        .unwrap();
    projects_table
        .insert(Row::new(vec![
            SqlValue::Integer(8),
            SqlValue::Varchar("Warehouse Automation".to_string()),
            SqlValue::Integer(5),
            SqlValue::Integer(410000),
        ]))
        .unwrap();

    let employees_table = db.get_table_mut("EMPLOYEES").unwrap();

    // Insert employees data matching expected results
    // From string-2 expected: Alice Johnson, Bob Smith, Carol White, David Brown, Eve Martinez, Frank Wilson, Grace Taylor, Henry Anderson
    employees_table
        .insert(Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Integer(1),
            SqlValue::Varchar("Alice".to_string()),
            SqlValue::Varchar("Johnson".to_string()),
            SqlValue::Varchar("Alice Johnson".to_string()),
            SqlValue::Varchar("Engineering".to_string()),
            SqlValue::Integer(1),
            SqlValue::Varchar("Senior Engineer".to_string()),
            SqlValue::Float(95000.0),
            SqlValue::Varchar("2020-01-15".to_string()),
            SqlValue::Null,
        ]))
        .unwrap();

    employees_table
        .insert(Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Integer(2),
            SqlValue::Varchar("Bob".to_string()),
            SqlValue::Varchar("Smith".to_string()),
            SqlValue::Varchar("Bob Smith".to_string()),
            SqlValue::Varchar("Sales".to_string()),
            SqlValue::Integer(2),
            SqlValue::Varchar("Sales Manager".to_string()),
            SqlValue::Float(85000.0),
            SqlValue::Varchar("2019-03-22".to_string()),
            SqlValue::Null,
        ]))
        .unwrap();

    employees_table
        .insert(Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Integer(3),
            SqlValue::Varchar("Carol".to_string()),
            SqlValue::Varchar("White".to_string()),
            SqlValue::Varchar("Carol White".to_string()),
            SqlValue::Varchar("Engineering".to_string()),
            SqlValue::Integer(1),
            SqlValue::Varchar("Engineer".to_string()),
            SqlValue::Float(75000.0),
            SqlValue::Varchar("2021-06-10".to_string()),
            SqlValue::Integer(1),
        ]))
        .unwrap();

    employees_table
        .insert(Row::new(vec![
            SqlValue::Integer(4),
            SqlValue::Integer(4),
            SqlValue::Varchar("David".to_string()),
            SqlValue::Varchar("Brown".to_string()),
            SqlValue::Varchar("David Brown".to_string()),
            SqlValue::Varchar("Marketing".to_string()),
            SqlValue::Integer(3),
            SqlValue::Varchar("Marketing Specialist".to_string()),
            SqlValue::Float(65000.0),
            SqlValue::Varchar("2022-01-05".to_string()),
            SqlValue::Integer(7),
        ]))
        .unwrap();

    employees_table
        .insert(Row::new(vec![
            SqlValue::Integer(5),
            SqlValue::Integer(5),
            SqlValue::Varchar("Eve".to_string()),
            SqlValue::Varchar("Martinez".to_string()),
            SqlValue::Varchar("Eve Martinez".to_string()),
            SqlValue::Varchar("Engineering".to_string()),
            SqlValue::Integer(1),
            SqlValue::Varchar("Senior Engineer".to_string()),
            SqlValue::Float(110000.0),
            SqlValue::Varchar("2018-09-12".to_string()),
            SqlValue::Null,
        ]))
        .unwrap();

    employees_table
        .insert(Row::new(vec![
            SqlValue::Integer(6),
            SqlValue::Integer(6),
            SqlValue::Varchar("Frank".to_string()),
            SqlValue::Varchar("Wilson".to_string()),
            SqlValue::Varchar("Frank Wilson".to_string()),
            SqlValue::Varchar("Sales".to_string()),
            SqlValue::Integer(2),
            SqlValue::Varchar("Sales Representative".to_string()),
            SqlValue::Float(55000.0),
            SqlValue::Varchar("2021-11-20".to_string()),
            SqlValue::Integer(2),
        ]))
        .unwrap();

    employees_table
        .insert(Row::new(vec![
            SqlValue::Integer(7),
            SqlValue::Integer(7),
            SqlValue::Varchar("Grace".to_string()),
            SqlValue::Varchar("Taylor".to_string()),
            SqlValue::Varchar("Grace Taylor".to_string()),
            SqlValue::Varchar("Marketing".to_string()),
            SqlValue::Integer(3),
            SqlValue::Varchar("Marketing Manager".to_string()),
            SqlValue::Float(90000.0),
            SqlValue::Varchar("2019-07-08".to_string()),
            SqlValue::Null,
        ]))
        .unwrap();

    employees_table
        .insert(Row::new(vec![
            SqlValue::Integer(8),
            SqlValue::Integer(8),
            SqlValue::Varchar("Henry".to_string()),
            SqlValue::Varchar("Anderson".to_string()),
            SqlValue::Varchar("Henry Anderson".to_string()),
            SqlValue::Varchar("Engineering".to_string()),
            SqlValue::Integer(1),
            SqlValue::Varchar("Engineer".to_string()),
            SqlValue::Float(80000.0),
            SqlValue::Varchar("2020-12-01".to_string()),
            SqlValue::Integer(1),
        ]))
        .unwrap();

    // Add additional employees with 'a' in first_name for string-7
    employees_table
        .insert(Row::new(vec![
            SqlValue::Integer(9),
            SqlValue::Integer(9),
            SqlValue::Varchar("Maria".to_string()),
            SqlValue::Varchar("Clark".to_string()),
            SqlValue::Varchar("Maria Clark".to_string()),
            SqlValue::Varchar("Sales".to_string()),
            SqlValue::Integer(2),
            SqlValue::Varchar("Sales Representative".to_string()),
            SqlValue::Float(58000.0),
            SqlValue::Varchar("2022-04-15".to_string()),
            SqlValue::Integer(2),
        ]))
        .unwrap();

    employees_table
        .insert(Row::new(vec![
            SqlValue::Integer(10),
            SqlValue::Integer(10),
            SqlValue::Varchar("Nathan".to_string()),
            SqlValue::Varchar("Lewis".to_string()),
            SqlValue::Varchar("Nathan Lewis".to_string()),
            SqlValue::Varchar("Engineering".to_string()),
            SqlValue::Integer(1),
            SqlValue::Varchar("Senior Engineer".to_string()),
            SqlValue::Float(105000.0),
            SqlValue::Varchar("2019-05-18".to_string()),
            SqlValue::Integer(5),
        ]))
        .unwrap();

    employees_table
        .insert(Row::new(vec![
            SqlValue::Integer(11),
            SqlValue::Integer(11),
            SqlValue::Varchar("Olivia".to_string()),
            SqlValue::Varchar("Walker".to_string()),
            SqlValue::Varchar("Olivia Walker".to_string()),
            SqlValue::Varchar("Marketing".to_string()),
            SqlValue::Integer(3),
            SqlValue::Varchar("Marketing Specialist".to_string()),
            SqlValue::Float(67000.0),
            SqlValue::Varchar("2021-08-22".to_string()),
            SqlValue::Integer(7),
        ]))
        .unwrap();

    employees_table
        .insert(Row::new(vec![
            SqlValue::Integer(12),
            SqlValue::Integer(12),
            SqlValue::Varchar("Paul".to_string()),
            SqlValue::Varchar("Hall".to_string()),
            SqlValue::Varchar("Paul Hall".to_string()),
            SqlValue::Varchar("Sales".to_string()),
            SqlValue::Integer(2),
            SqlValue::Varchar("Sales Representative".to_string()),
            SqlValue::Float(56000.0),
            SqlValue::Varchar("2023-02-10".to_string()),
            SqlValue::Integer(2),
        ]))
        .unwrap();

    db
}

/// Create a University database for testing
pub fn create_university_db() -> Database {
    let mut db = Database::new();

    // Create students table
    let students_schema = TableSchema::new(
        "STUDENTS".to_string(),
        vec![
            ColumnSchema::new("STUDENT_ID".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "NAME".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
            ColumnSchema::new(
                "MAJOR".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new("GPA".to_string(), DataType::Float { precision: 53 }, false),
        ],
    );
    db.create_table(students_schema).unwrap();

    // Create courses table
    let courses_schema = TableSchema::new(
        "COURSES".to_string(),
        vec![
            ColumnSchema::new("COURSE_ID".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "COURSE_NAME".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
            ColumnSchema::new(
                "DEPARTMENT".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new("CREDITS".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(courses_schema).unwrap();

    // Create enrollments table
    let enrollments_schema = TableSchema::new(
        "ENROLLMENTS".to_string(),
        vec![
            ColumnSchema::new("STUDENT_ID".to_string(), DataType::Integer, false),
            ColumnSchema::new("COURSE_ID".to_string(), DataType::Integer, false),
            ColumnSchema::new("GRADE".to_string(), DataType::Varchar { max_length: Some(2) }, true), // nullable
            ColumnSchema::new(
                "SEMESTER".to_string(),
                DataType::Varchar { max_length: Some(20) },
                false,
            ),
        ],
    );
    db.create_table(enrollments_schema).unwrap();

    // Insert students
    let students_table = db.get_table_mut("STUDENTS").unwrap();
    for i in 1..=20 {
        let (name, major, gpa) = match i {
            1 => ("Alice Johnson", "Computer Science", 3.8_f32),
            2 => ("Bob Smith", "Mathematics", 3.5_f32),
            3 => ("Carol White", "Computer Science", 3.9_f32),
            4 => ("David Brown", "Physics", 3.2_f32),
            5 => ("Eve Martinez", "Computer Science", 3.7_f32),
            6 => ("Frank Wilson", "Mathematics", 3.4_f32),
            7 => ("Grace Taylor", "Physics", 3.6_f32),
            8 => ("Henry Anderson", "Computer Science", 3.3_f32),
            9 => ("Iris Chen", "Mathematics", 3.8_f32),
            10 => ("Jack Robinson", "Physics", 3.1_f32),
            _ => ("Student", "Computer Science", 3.0_f32 + (i as f32 % 10.0) / 10.0),
        };
        students_table
            .insert(Row::new(vec![
                SqlValue::Integer(i),
                SqlValue::Varchar(name.to_string()),
                SqlValue::Varchar(major.to_string()),
                SqlValue::Float(gpa),
            ]))
            .unwrap();
    }

    // Insert courses
    let courses_table = db.get_table_mut("COURSES").unwrap();
    let course_data = vec![
        (101, "Introduction to Programming", "Computer Science", 4),
        (102, "Data Structures", "Computer Science", 4),
        (103, "Algorithms", "Computer Science", 4),
        (201, "Calculus I", "Mathematics", 4),
        (202, "Linear Algebra", "Mathematics", 3),
        (203, "Statistics", "Mathematics", 3),
        (301, "Classical Mechanics", "Physics", 4),
        (302, "Electromagnetism", "Physics", 4),
        (303, "Quantum Mechanics", "Physics", 3),
    ];

    for (id, name, dept, credits) in course_data {
        courses_table
            .insert(Row::new(vec![
                SqlValue::Integer(id),
                SqlValue::Varchar(name.to_string()),
                SqlValue::Varchar(dept.to_string()),
                SqlValue::Integer(credits),
            ]))
            .unwrap();
    }

    // Insert enrollments - matching grade distribution from uni-4:
    // A: 30, B: 27, C: 18, D: 6, F: 2 (total: 83 non-NULL grades)
    let enrollments_table = db.get_table_mut("ENROLLMENTS").unwrap();

    let grade_distribution = vec![("A", 30), ("B", 27), ("C", 18), ("D", 6), ("F", 2)];

    let mut enrollment_id = 0;
    for (grade, count) in grade_distribution {
        for _ in 0..count {
            let student_id = (enrollment_id % 20) + 1;
            let course_id = match enrollment_id % 9 {
                0 => 101,
                1 => 102,
                2 => 103,
                3 => 201,
                4 => 202,
                5 => 203,
                6 => 301,
                7 => 302,
                _ => 303,
            };

            enrollments_table
                .insert(Row::new(vec![
                    SqlValue::Integer(student_id),
                    SqlValue::Integer(course_id),
                    SqlValue::Varchar(grade.to_string()),
                    SqlValue::Varchar("Fall 2024".to_string()),
                ]))
                .unwrap();

            enrollment_id += 1;
        }
    }

    // Add some NULL grades (in progress courses)
    for i in 0..10 {
        let student_id = (i % 20) + 1;
        let course_id = 101 + (i % 3);
        enrollments_table
            .insert(Row::new(vec![
                SqlValue::Integer(student_id),
                SqlValue::Integer(course_id),
                SqlValue::Null,
                SqlValue::Varchar("Spring 2025".to_string()),
            ]))
            .unwrap();
    }

    db
}

/// Create an empty database for testing
pub fn create_empty_db() -> Database {
    Database::new()
}

/// Load the appropriate database for a given example
pub fn load_database(db_name: &str) -> Option<Database> {
    match db_name {
        "northwind" => Some(create_northwind_db()),
        "employees" | "company" => Some(create_employees_db()),
        "university" => Some(create_university_db()),
        "empty" => Some(create_empty_db()),
        _ => None,
    }
}
