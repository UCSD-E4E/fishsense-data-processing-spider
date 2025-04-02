WITH paths_to_update AS (
SELECT canonical_dives.path
FROM dives
LEFT JOIN canonical_dives ON canonical_dives.path = dives.path
WHERE dives.checksum = %(checksum)s
AND dives.checksum != canonical_dives.checksum
ORDER BY dives.path
)
UPDATE canonical_dives
SET checksum = %(checksum)s
FROM paths_to_update
WHERE canonical_dives.path = paths_to_update.path
RETURNING canonical_dives.path
;