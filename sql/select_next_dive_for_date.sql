SELECT path
FROM dives
WHERE dives.date IS NULL
LIMIT 1
;