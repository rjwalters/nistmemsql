# Subquery Architecture Diagrams

## 1. Expression Evaluator State Machine

```
┌─────────────────────────────────────────────────────────────────┐
│                    ExpressionEvaluator<'a>                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Fields:                                                        │
│  ┌─────────────────────────────────────────────────────────┐  │
│  │ schema: &'a TableSchema                                 │  │
│  │   - Schema of current/inner table                       │  │
│  │   - Used for single-table queries                       │  │
│  └─────────────────────────────────────────────────────────┘  │
│                           │                                    │
│                           ▼                                    │
│  ┌─────────────────────────────────────────────────────────┐  │
│  │ outer_row: Option<&'a Row>                              │  │
│  │   - Current row from outer/parent query                 │  │
│  │   - Used in correlated subqueries                       │  │
│  │   - Currently mostly unused (TODO: implement)           │  │
│  └─────────────────────────────────────────────────────────┘  │
│                           │                                    │
│                           ▼                                    │
│  ┌─────────────────────────────────────────────────────────┐  │
│  │ outer_schema: Option<&'a TableSchema>                   │  │
│  │   - Schema of outer query's FROM clause                 │  │
│  │   - Needed to resolve outer column references           │  │
│  │   - Currently mostly unused (TODO: implement)           │  │
│  └─────────────────────────────────────────────────────────┘  │
│                           │                                    │
│                           ▼                                    │
│  ┌─────────────────────────────────────────────────────────┐  │
│  │ database: Option<&'a Database>                          │  │
│  │   - Reference to entire database for subqueries         │  │
│  │   - Used in scalar subquery execution                   │  │
│  │   - Currently active and working                        │  │
│  └─────────────────────────────────────────────────────────┘  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## 2. Column Resolution Pipeline

```
Column Reference Resolution
(For: ColumnRef { table: Option<String>, column: String })

                         ┌─────────────────────┐
                         │  ColumnRef found?   │
                         │  (table, column)    │
                         └──────────┬──────────┘
                                    │
                    ┌───────────────┴───────────────┐
                    │                               │
                    ▼                               ▼
            Is table qualified?             Unqualified column
            (Some(table_name))              (table = None)
                    │                               │
        ┌───────────┴──────────┐                    │
        │                      │                    │
        ▼                      ▼                    ▼
  TABLE QUALIFIED      TABLE NOT FOUND      Search order:
  ┌──────────────┐     ┌──────────────┐    
  │ Look in      │     │ Error:       │    1. Try inner schema
  │ specified    │     │ Table not    │       (current table)
  │ table's      │     │ found        │       
  │ schema       │     └──────────────┘    2. If not found AND
  └──────┬───────┘                            outer_row exists:
         │                                    Try outer schema
         ▼                                    (parent query)
  ┌──────────────┐     
  │ Calculate:   │     3. Still not found?
  │ start_index  │       ├─> Error:
  │   + col_idx  │       │   ColumnNotFound
  └──────┬───────┘       │
         │               └─> Return value
         ▼                   from outer_row
    Return value
    from row[index]
         │
         ▼
      SqlValue
```

## 3. Scalar Subquery Execution Flow

```
Outer Query Execution
│
├─► execute(SELECT stmt)
│   │
│   ├─► Build FROM clause result
│   │
│   └─► For each row in FROM:
│       │
│       ├─► Evaluate WHERE clause
│       │   │
│       │   └─► eval(expr, current_row)
│       │       │
│       │       ├─► BinaryOp { left, op, right }
│       │       │   │
│       │       │   ├─► eval(left, row) ──┐
│       │       │   │                      ├─► Compare
│       │       │   └─► eval(right, row)──┘
│       │       │       │
│       │       │       └─► If right is ScalarSubquery:
│       │       │           │
│       │       │           ├─► Get database reference
│       │       │           │   from evaluator.database
│       │       │           │
│       │       │           ├─► Create new SelectExecutor
│       │       │           │
│       │       │           ├─► Execute subquery
│       │       │           │   (WITHOUT outer context)
│       │       │           │
│       │       │           ├─► Validate:
│       │       │           │   - Exactly 1 row
│       │       │           │   - Exactly 1 column
│       │       │           │
│       │       │           └─► Return single value
│       │       │               or NULL
│       │       │
│       │       └─► Result: Boolean(true/false)
│       │
│       └─► Include row if WHERE = true


❌ CURRENT PROBLEM:
   Subquery executes WITHOUT outer context
   - outer_row is always None
   - outer_schema is always None
   - Cannot reference parent query columns

✅ SOLUTION NEEDED:
   Pass outer context to subquery:
   - Create evaluator with outer context
   - Thread through entire evaluation chain
   - Enable column correlation
```

## 4. Data Structure Relationships

```
Database
├── tables: HashMap<String, Table>
│
└── Table ("employees")
    ├── schema: TableSchema
    │   ├── name: "employees"
    │   └── columns: Vec<ColumnSchema>
    │       ├── ColumnSchema { name: "id", type: Integer, nullable: false }
    │       ├── ColumnSchema { name: "name", type: Varchar, nullable: true }
    │       └── ColumnSchema { name: "salary", type: Integer, nullable: false }
    │
    └── rows: Vec<Row>
        ├── Row { values: [Integer(1), Varchar("Alice"), Integer(50000)] }
        ├── Row { values: [Integer(2), Varchar("Bob"), Integer(60000)] }
        └── Row { values: [Integer(3), Varchar("Charlie"), Integer(70000)] }


For Joins (CombinedSchema):
┌────────────────────────────────────────────────────────┐
│ CombinedSchema                                         │
├────────────────────────────────────────────────────────┤
│ table_schemas: HashMap<String, (usize, TableSchema)>  │
│                                                        │
│ "employees" ─► (0, TableSchema {                       │
│                  name: "employees",                    │
│                  columns: [id, name, salary]           │
│                })                                      │
│                                                        │
│ "orders" ──► (3, TableSchema {                         │
│              name: "orders",                           │
│              columns: [order_id, emp_id, amount]       │
│            })                                          │
│                                                        │
│ total_columns: 6                                       │
└────────────────────────────────────────────────────────┘

Combined Row values:
[Alice_id, Alice_name, Alice_sal, Order1_id, Order1_emp, Order1_amt]
 0         1           2           3          4           5
                ^─────────┬─────────^
                Start indices track table boundaries
```

## 5. AST Structure for Subqueries

```
SelectStmt (Outer Query)
├── select_list: [
│   └── SelectItem::Expression { expr, alias }
│       └── expr: ColumnRef { table: None, column: "id" }
│   ]
│
├── from: Some(FromClause::Table { name: "employees", alias: None })
│
├── where_clause: Some(
│   └── Expression::BinaryOp {
│       left: Box(ColumnRef { column: "salary" }),
│       op: GreaterThan,
│       right: Box(ScalarSubquery {
│           ┌────────────────────────────┐
│           │ SelectStmt (Subquery)      │ ← Nested SELECT
│           ├────────────────────────────┤
│           │ select_list: [             │
│           │   Function {               │
│           │     name: "AVG",            │
│           │     args: [ColumnRef]       │
│           │   }                         │
│           │ ]                           │
│           │                            │
│           │ from: Some(Table {          │
│           │   name: "employees"         │
│           │ })                          │
│           │                            │
│           │ where_clause: None          │
│           └────────────────────────────┘
│       })
│   }
│ )
│
└── limit: None
```

## 6. Correlated Subquery Desired State

```
                         CURRENT STATE
┌────────────────────────────────────────────────┐
│ ExpressionEvaluator                            │
├────────────────────────────────────────────────┤
│ For scalar subquery:                           │
│                                                │
│ eval(ScalarSubquery(stmt), row) {              │
│   let database = self.database?;               │
│   let executor = SelectExecutor::new(database);│
│   let rows = executor.execute(stmt)?;   ← NO  │
│                                           CONTEXT│
│   ...                                          │
│ }                                              │
│                                                │
│ Column resolution in subquery:                 │
│ outer_row = None  (CAN'T ACCESS)              │
│ outer_schema = None (CAN'T ACCESS)            │
└────────────────────────────────────────────────┘

                       DESIRED STATE
┌────────────────────────────────────────────────┐
│ ExpressionEvaluator                            │
├────────────────────────────────────────────────┤
│ For scalar subquery:                           │
│                                                │
│ eval(ScalarSubquery(stmt), row) {              │
│   let database = self.database?;               │
│   let executor = SelectExecutor::new(database);│
│                                                │
│   // Option 1: Pass context to SelectExecutor │
│   let rows = executor.execute_with_context(   │
│       stmt,                                    │
│       self.outer_row,      ← PASS CONTEXT    │
│       self.outer_schema     ← PASS CONTEXT    │
│   )?;                                          │
│                                                │
│   // Option 2: Create child evaluator         │
│   let child = ExpressionEvaluator {            │
│       schema: subquery_schema,                 │
│       outer_row: row,       ← CURRENT BECOMES │
│       outer_schema: self.schema,  ← OUTER    │
│       database: self.database,                │
│   };                                           │
│   ...                                          │
│ }                                              │
│                                                │
│ Column resolution in subquery:                 │
│ outer_row = Some(row)    (CAN ACCESS)        │
│ outer_schema = Some(schema) (CAN ACCESS)     │
└────────────────────────────────────────────────┘
```

## 7. Evaluation Context Nesting

```
┌─────────────────────────────────────────────────────────┐
│ Level 1: OUTER QUERY                                    │
│ SELECT id FROM employees WHERE salary > (subquery)     │
│                                                         │
│ Context:                                                │
│ ├── table: "employees"                                  │
│ ├── outer_row: None (no parent query)                   │
│ ├── outer_schema: None (no parent query)                │
│ └── database: &db                                       │
│                                                         │
│     For row = [1, "Alice", 50000]:                      │
│     eval(salary > (subquery), row)                      │
│     │                                                   │
│     └──────────────────┐                                │
│                        │                                │
│        ┌───────────────▼──────────────────────────┐    │
│        │ Level 2: SUBQUERY                        │    │
│        │ SELECT AVG(salary) FROM employees       │    │
│        │                                         │    │
│        │ Context:                                │    │
│        │ ├── table: "employees"                  │    │
│        │ ├── outer_row: Some(row)  ← ALICE      │    │
│        │ ├── outer_schema: employees_schema     │    │
│        │ └── database: &db                       │    │
│        │                                         │    │
│        │ For correlation like:                   │    │
│        │ "WHERE salary > (SELECT ... WHERE ...   │    │
│        │   emp.id = employees.id)"               │    │
│        │                                         │    │
│        │ Column "employees.id" resolves to:      │    │
│        │ outer_row.get(0) = 1  ← CORRELATED     │    │
│        │                                         │    │
│        └─────────────────────────────────────────┘    │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

## 8. IN Operator Subquery Execution (Pending)

```
eval(In { expr, subquery, negated })
│
├─► Evaluate left expression
│   left_val = eval(expr, row)
│
├─► Get database
│   database = self.database?
│
├─► Execute subquery
│   rows = SelectExecutor::new(database).execute(subquery)?
│
├─► Collect values
│   values = rows.iter().map(|r| r.get(0)).collect()
│
├─► Membership test
│   found = values.iter().any(|v| v == left_val)
│
├─► Apply negation
│   result = if negated { !found } else { found }
│
└─► Return result
    Ok(Boolean(result))


Special Case: NULL Semantics
SQL:1999 Three-Valued Logic for IN:

value IN (set)
├─► If value found in set: TRUE
├─► If value not found and no NULLs: FALSE
├─► If value not found but NULLs exist: NULL
└─► If value is NULL: NULL (always)

Example:
1 IN (1, 2, 3) → TRUE
4 IN (1, 2, 3) → FALSE
1 IN (1, NULL, 3) → TRUE
4 IN (1, NULL, 3) → NULL ← Tricky!
NULL IN (1, 2, 3) → NULL
NULL IN (NULL) → NULL
```

## 9. Test Scenario Coverage Map

```
Subquery Types Implemented:

✅ SCALAR SUBQUERY
   ├─ Non-Correlated
   │  ├─ In WHERE clause
   │  │  └─ Comparison operators (>, <, =, etc.)
   │  ├─ In SELECT list
   │  ├─ Empty result (returns NULL)
   │  └─ Error cases (too many rows, too many columns)
   │
   └─ Correlated (TODO)
      ├─ Reference outer column in WHERE
      ├─ Reference outer column in FROM
      └─ Reference outer column in aggregate

✅ FROM CLAUSE SUBQUERY (DERIVED TABLE)
   ├─ Basic SELECT subquery
   ├─ With aliases
   ├─ Type inference
   └─ Joins with derived tables (TODO)

⚠️ IN OPERATOR SUBQUERY
   ├─ Non-Correlated
   │  ├─ IN (SELECT ...)
   │  ├─ NOT IN (SELECT ...)
   │  └─ NULL semantics (TODO)
   │
   └─ Correlated (TODO)
      └─ Reference outer in subquery

🔄 CORRELATED SUBQUERIES (IN PROGRESS)
   ├─ In WHERE clause
   ├─ In SELECT list
   ├─ In aggregate functions (TODO)
   ├─ Multiple correlation levels (TODO)
   └─ With JOINs (TODO)
```

## 10. Code Flow Diagram

```
main() or test
  │
  └─► Parser::parse_sql(sql_string)
      └─► AST: SelectStmt with expressions
  
  │
  └─► Database::new() or use existing
  
  │
  └─► SelectExecutor::new(&database)
      │
      └─► .execute(&stmt) : Vec<Row>
          │
          ├─► Check for aggregates/GROUP BY
          │
          └─► execute_without_aggregation(stmt, from_result)
              │
              ├─► execute_from(FromClause)
              │   │
              │   ├─► Table: scan & return rows
              │   │
              │   ├─► Subquery:
              │   │   └─► Recursive execute() call
              │   │       └─► SelectExecutor::execute(subquery_stmt)
              │   │
              │   └─► Join: nested_loop_join()
              │
              ├─► For each row:
              │   │
              │   ├─► Apply WHERE filter
              │   │   └─► CombinedExpressionEvaluator::eval(where_expr, row)
              │   │       │
              │   │       └─► If expr contains ScalarSubquery:
              │   │           │
              │   │           ├─► eval(ScalarSubquery(subquery_stmt), row)
              │   │           │   └─► Create SelectExecutor
              │   │           │       └─► Execute subquery
              │   │           │           └─► Get single value
              │   │           │
              │   │           └─► Use value in comparison
              │   │
              │   └─► Project SELECT list
              │       └─► project_row_combined()
              │
              └─► Apply DISTINCT, ORDER BY, LIMIT
```

