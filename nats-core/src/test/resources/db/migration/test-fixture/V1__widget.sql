-- Test-only fixture for SqlMigrationRunnerTest — not part of the basamak-2 schema.
CREATE TABLE widget (
    id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(64) NOT NULL
);
