SELECT images.image_md5 as cksum
FROM images
INNER JOIN canonical_dives ON canonical_dives.path = images.dive
LEFT JOIN headtail_labels ON headtail_labels.cksum = images.image_md5
WHERE images.preprocess_laser_jpeg_path IS NOT NULL AND
    headtail_labels.cksum IS NULL AND
    canonical_dives.priority = %(priority)s
;