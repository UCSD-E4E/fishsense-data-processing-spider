SELECT d.path, d.date, d.invalid_image, d.multiple_date, d.data_path, d.checksum
FROM dives as d
LEFT JOIN canonical_dives ON d.checksum = canonical_dives.checksum
WHERE d.checksum = %(checksum)s AND canonical_dives.checksum is NULL
ORDER BY d.path
LIMIT 1
;