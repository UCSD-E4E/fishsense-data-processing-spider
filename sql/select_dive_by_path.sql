SELECT path
FROM dives
WHERE dives.path = %(path)s
LIMIT 1
;