CREATE PROCEDURE test_proc (IN p_id INT)
BEGIN
  SELECT p_id AS id;
END;

CALL test_proc(1);
