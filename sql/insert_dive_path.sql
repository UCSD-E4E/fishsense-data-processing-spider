INSERT INTO dives (
    path
) VALUES (
    %(path)s
)
ON CONFLICT DO NOTHING
;