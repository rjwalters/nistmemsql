-- Test queries from issue #1171
-- These should now format with .000 suffix

-- Example 1: COUNT should format as 11.000
SELECT ALL - ( + - 10 ) + COUNT( 61 );

-- Example 2: Should format with .000 suffix
SELECT COUNT( * ) AS col2, + 39 * - + 79 * - - MIN( - 5 ) * + 3 + + ( 45 ) + + - SUM( ALL 32 ) AS col0;
