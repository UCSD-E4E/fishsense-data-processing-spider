CREATE TABLE cameras (
    "idx" INT PRIMARY KEY,
    "serial_number" TEXT,
    "name" TEXT
);

ALTER TABLE dives
ADD camera INT NULL REFERENCES cameras;
ALTER TABLE canonical_dives
ADD camera INT NULL REFERENCES cameras;