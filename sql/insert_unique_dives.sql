INSERT INTO canonical_dives (path, date, invalid_image, multiple_date, checksum, data_path)
    SELECT min(path) as path,
        min(date) as date,
        bool_or(invalid_image) as invalid_image,
        bool_or(multiple_date) as multiple_date,
        checksum,
        min(data_path) as data_path
    FROM dives
    GROUP BY checksum
ON CONFLICT DO NOTHING;