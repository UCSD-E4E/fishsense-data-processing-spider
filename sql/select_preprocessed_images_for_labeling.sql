SELECT images.image_md5 as cksum
FROM images
INNER JOIN canonical_dives ON canonical_dives.path = images.dive
LEFT JOIN laser_labels ON laser_labels.cksum = images.image_md5
WHERE images.preprocess_jpeg_path IS NOT NULL AND
    laser_labels.cksum IS NULL AND
    canonical_dives.priority = %(priority)s
;