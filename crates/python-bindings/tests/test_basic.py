"""
Basic tests for nistmemsql Python bindings
"""
import nistmemsql


def test_connection():
    """Test basic connection creation"""
    db = nistmemsql.connect()
    assert db is not None
    db.close()


def test_cursor_creation():
    """Test cursor creation from database"""
    db = nistmemsql.connect()
    cursor = db.cursor()
    assert cursor is not None
    cursor.close()
    db.close()


def test_create_table():
    """Test CREATE TABLE statement"""
    db = nistmemsql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER, name VARCHAR(50))")
    cursor.close()
    db.close()


def test_insert_select():
    """Test INSERT and SELECT operations"""
    db = nistmemsql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER, name VARCHAR(50))")
    cursor.execute("INSERT INTO users VALUES (1, 'Alice')")
    cursor.execute("SELECT * FROM users")
    rows = cursor.fetchall()
    assert len(rows) == 1
    assert rows[0] == (1, 'Alice')
    cursor.close()
    db.close()


def test_multiple_inserts():
    """Test multiple INSERT operations"""
    db = nistmemsql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER, name VARCHAR(50))")
    cursor.execute("INSERT INTO users VALUES (1, 'Alice')")
    cursor.execute("INSERT INTO users VALUES (2, 'Bob')")
    cursor.execute("INSERT INTO users VALUES (3, 'Charlie')")
    cursor.execute("SELECT * FROM users")
    rows = cursor.fetchall()
    assert len(rows) == 3
    assert rows[0] == (1, 'Alice')
    assert rows[1] == (2, 'Bob')
    assert rows[2] == (3, 'Charlie')
    cursor.close()
    db.close()


def test_data_types():
    """Test various SQL data types"""
    db = nistmemsql.connect()
    cursor = db.cursor()
    # Create a dummy table for testing various data types
    cursor.execute("CREATE TABLE datatypes (i INTEGER, f FLOAT, s VARCHAR(50), b BOOLEAN, n INTEGER)")
    cursor.execute("INSERT INTO datatypes VALUES (42, 3.14, 'hello', TRUE, NULL)")
    cursor.execute("SELECT * FROM datatypes")
    row = cursor.fetchone()
    assert row[0] == 42
    assert abs(row[1] - 3.14) < 0.01  # Float comparison with tolerance
    assert row[2] == 'hello'
    assert row[3] == True
    assert row[4] is None
    cursor.close()
    db.close()


def test_fetchone():
    """Test fetchone() method"""
    db = nistmemsql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE numbers (n INTEGER)")
    cursor.execute("INSERT INTO numbers VALUES (1)")
    cursor.execute("INSERT INTO numbers VALUES (2)")
    cursor.execute("INSERT INTO numbers VALUES (3)")
    cursor.execute("SELECT * FROM numbers")

    row1 = cursor.fetchone()
    assert row1 == (1,)

    row2 = cursor.fetchone()
    assert row2 == (2,)

    row3 = cursor.fetchone()
    assert row3 == (3,)

    row4 = cursor.fetchone()
    assert row4 is None

    cursor.close()
    db.close()


def test_fetchmany():
    """Test fetchmany() method"""
    db = nistmemsql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE numbers (n INTEGER)")
    for i in range(10):
        cursor.execute(f"INSERT INTO numbers VALUES ({i})")

    cursor.execute("SELECT * FROM numbers")

    rows = cursor.fetchmany(3)
    assert len(rows) == 3
    assert rows[0] == (0,)
    assert rows[1] == (1,)
    assert rows[2] == (2,)

    rows = cursor.fetchmany(5)
    assert len(rows) == 5

    cursor.close()
    db.close()


def test_update():
    """Test UPDATE statement"""
    db = nistmemsql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER, name VARCHAR(50))")
    cursor.execute("INSERT INTO users VALUES (1, 'Alice')")
    cursor.execute("UPDATE users SET name = 'Alicia' WHERE id = 1")
    cursor.execute("SELECT * FROM users")
    row = cursor.fetchone()
    assert row == (1, 'Alicia')
    cursor.close()
    db.close()


def test_delete():
    """Test DELETE statement"""
    db = nistmemsql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER, name VARCHAR(50))")
    cursor.execute("INSERT INTO users VALUES (1, 'Alice')")
    cursor.execute("INSERT INTO users VALUES (2, 'Bob')")
    cursor.execute("DELETE FROM users WHERE id = 1")
    cursor.execute("SELECT * FROM users")
    rows = cursor.fetchall()
    assert len(rows) == 1
    assert rows[0] == (2, 'Bob')
    cursor.close()
    db.close()


def test_rowcount():
    """Test rowcount attribute"""
    db = nistmemsql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER, name VARCHAR(50))")
    cursor.execute("INSERT INTO users VALUES (1, 'Alice')")
    assert cursor.rowcount == 1

    cursor.execute("INSERT INTO users VALUES (2, 'Bob')")
    assert cursor.rowcount == 1

    cursor.execute("SELECT * FROM users")
    cursor.fetchall()
    assert cursor.rowcount == 2

    cursor.close()
    db.close()


def test_drop_table():
    """Test DROP TABLE statement"""
    db = nistmemsql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE temp (id INTEGER)")
    cursor.execute("DROP TABLE temp")
    cursor.close()
    db.close()


def test_multiple_cursors():
    """Test multiple cursors on same database"""
    db = nistmemsql.connect()

    cursor1 = db.cursor()
    cursor2 = db.cursor()

    cursor1.execute("CREATE TABLE users (id INTEGER, name VARCHAR(50))")
    cursor1.execute("INSERT INTO users VALUES (1, 'Alice')")

    cursor2.execute("SELECT * FROM users")
    rows = cursor2.fetchall()
    assert len(rows) == 1
    assert rows[0] == (1, 'Alice')

    cursor1.close()
    cursor2.close()
    db.close()


# ============================================================================
# Parameterized Query Tests (Issue #824)
# ============================================================================

def test_parameterized_select():
    """Test basic parameterized SELECT with single parameter"""
    db = nistmemsql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER, name VARCHAR(50))")
    cursor.execute("INSERT INTO users VALUES (1, 'Alice')")
    cursor.execute("INSERT INTO users VALUES (2, 'Bob')")
    cursor.execute("INSERT INTO users VALUES (3, 'Charlie')")

    # Parameterized SELECT
    cursor.execute("SELECT * FROM users WHERE id = ?", (2,))
    rows = cursor.fetchall()
    assert len(rows) == 1
    assert rows[0] == (2, 'Bob')

    cursor.close()
    db.close()


def test_parameterized_insert():
    """Test parameterized INSERT statement"""
    db = nistmemsql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER, name VARCHAR(50))")

    # Parameterized INSERT
    cursor.execute("INSERT INTO users VALUES (?, ?)", (1, 'Alice'))
    cursor.execute("SELECT * FROM users")
    rows = cursor.fetchall()
    assert len(rows) == 1
    assert rows[0] == (1, 'Alice')

    cursor.close()
    db.close()


def test_parameterized_update():
    """Test parameterized UPDATE statement"""
    db = nistmemsql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER, name VARCHAR(50))")
    cursor.execute("INSERT INTO users VALUES (1, 'Alice')")
    cursor.execute("INSERT INTO users VALUES (2, 'Bob')")

    # Parameterized UPDATE
    cursor.execute("UPDATE users SET name = ? WHERE id = ?", ('Alicia', 1))
    cursor.execute("SELECT * FROM users WHERE id = 1")
    row = cursor.fetchone()
    assert row == (1, 'Alicia')

    cursor.close()
    db.close()


def test_parameterized_delete():
    """Test parameterized DELETE statement"""
    db = nistmemsql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER, name VARCHAR(50))")
    cursor.execute("INSERT INTO users VALUES (1, 'Alice')")
    cursor.execute("INSERT INTO users VALUES (2, 'Bob')")
    cursor.execute("INSERT INTO users VALUES (3, 'Charlie')")

    # Parameterized DELETE
    cursor.execute("DELETE FROM users WHERE id = ?", (2,))
    cursor.execute("SELECT * FROM users")
    rows = cursor.fetchall()
    assert len(rows) == 2
    assert rows[0] == (1, 'Alice')
    assert rows[1] == (3, 'Charlie')

    cursor.close()
    db.close()


def test_parameterized_multiple_types():
    """Test parameterized query with multiple parameter types"""
    db = nistmemsql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE data (id INTEGER, value FLOAT, name VARCHAR(50), active BOOLEAN)")

    # Insert with multiple types
    cursor.execute("INSERT INTO data VALUES (?, ?, ?, ?)", (1, 3.14, 'test', True))
    cursor.execute("SELECT * FROM data")
    row = cursor.fetchone()
    assert row[0] == 1
    assert abs(row[1] - 3.14) < 0.01
    assert row[2] == 'test'
    assert row[3] == True

    cursor.close()
    db.close()


def test_parameterized_null_value():
    """Test parameterized query with NULL parameter"""
    db = nistmemsql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER, name VARCHAR(50))")

    # Insert with NULL
    cursor.execute("INSERT INTO users VALUES (?, ?)", (1, None))
    cursor.execute("SELECT * FROM users")
    row = cursor.fetchone()
    assert row[0] == 1
    assert row[1] is None

    cursor.close()
    db.close()


def test_parameterized_string_with_quotes():
    """Test parameterized query with string containing quotes"""
    db = nistmemsql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER, name VARCHAR(50))")

    # Insert string with single quotes
    cursor.execute("INSERT INTO users VALUES (?, ?)", (1, "O'Brien"))
    cursor.execute("SELECT * FROM users")
    row = cursor.fetchone()
    assert row[0] == 1
    assert row[1] == "O'Brien"

    cursor.close()
    db.close()


def test_parameterized_error_count_mismatch():
    """Test error when parameter count doesn't match placeholders"""
    db = nistmemsql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER, name VARCHAR(50))")

    # Too few parameters
    try:
        cursor.execute("INSERT INTO users VALUES (?, ?)", (1,))
        assert False, "Should have raised ProgrammingError"
    except nistmemsql.ProgrammingError as e:
        assert "Parameter count mismatch" in str(e)

    # Too many parameters
    try:
        cursor.execute("INSERT INTO users VALUES (?, ?)", (1, 'Alice', 'Extra'))
        assert False, "Should have raised ProgrammingError"
    except nistmemsql.ProgrammingError as e:
        assert "Parameter count mismatch" in str(e)

    cursor.close()
    db.close()


def test_parameterized_error_invalid_type():
    """Test error when parameter has invalid type"""
    db = nistmemsql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER, name VARCHAR(50))")

    # Try to insert invalid type (dict)
    try:
        cursor.execute("INSERT INTO users VALUES (?, ?)", (1, {'invalid': 'dict'}))
        assert False, "Should have raised ProgrammingError"
    except nistmemsql.ProgrammingError as e:
        assert "invalid type" in str(e).lower() or "cannot convert" in str(e).lower()

    cursor.close()
    db.close()


def test_backward_compatibility_no_params():
    """Test backward compatibility - queries without parameters still work"""
    db = nistmemsql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER, name VARCHAR(50))")
    cursor.execute("INSERT INTO users VALUES (1, 'Alice')")
    cursor.execute("SELECT * FROM users")
    rows = cursor.fetchall()
    assert len(rows) == 1
    assert rows[0] == (1, 'Alice')

    cursor.close()
    db.close()


def test_parameterized_complex_where():
    """Test parameterized query with multiple conditions"""
    db = nistmemsql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER, age INTEGER, active BOOLEAN)")
    cursor.execute("INSERT INTO users VALUES (1, 25, TRUE)")
    cursor.execute("INSERT INTO users VALUES (2, 30, FALSE)")
    cursor.execute("INSERT INTO users VALUES (3, 35, TRUE)")
    cursor.execute("INSERT INTO users VALUES (4, 40, TRUE)")

    # Query with multiple parameters
    cursor.execute("SELECT * FROM users WHERE age > ? AND active = ?", (28, True))
    rows = cursor.fetchall()
    assert len(rows) == 2
    assert rows[0][0] == 3  # id=3, age=35
    assert rows[1][0] == 4  # id=4, age=40

    cursor.close()
    db.close()


if __name__ == "__main__":
    # Run all tests
    test_connection()
    test_cursor_creation()
    test_create_table()
    test_insert_select()
    test_multiple_inserts()
    test_data_types()
    test_fetchone()
    test_fetchmany()
    test_update()
    test_delete()
    test_rowcount()
    test_drop_table()
    test_multiple_cursors()

    # Parameterized query tests
    test_parameterized_select()
    test_parameterized_insert()
    test_parameterized_update()
    test_parameterized_delete()
    test_parameterized_multiple_types()
    test_parameterized_null_value()
    test_parameterized_string_with_quotes()
    test_parameterized_error_count_mismatch()
    test_parameterized_error_invalid_type()
    test_backward_compatibility_no_params()
    test_parameterized_complex_where()

    print("All tests passed!")
