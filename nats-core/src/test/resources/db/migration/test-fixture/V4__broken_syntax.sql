-- Test-only fixture for SqlMigrationRunnerTest -- intentionally invalid SQL syntax, used to
-- exercise the generic (non-already-exists) SQLException wrapping branch of applyClasspathScript.
CREATE TABLEEE this is not valid sql;
