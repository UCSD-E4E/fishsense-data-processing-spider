SELECT camera_idx, array_agg(cksum) as checksums
FROM (
  SELECT images.image_md5 as cksum, cameras.idx as camera_idx
  FROM images
  INNER JOIN canonical_dives ON images.dive = canonical_dives.path
  RIGHT JOIN laser_labels ON images.image_md5 = laser_labels.cksum
  LEFT JOIN priorities ON canonical_dives.priority = priorities.name
  LEFT JOIN headtail_labels ON images.image_md5 = headtail_labels.cksum
  LEFT JOIN cameras ON images.camera_sn = cameras.serial_number
  WHERE laser_labels.complete = TRUE AND
    images.preprocess_laser_jpeg_path IS NULL AND
    images.ignore = FALSE AND
    canonical_dives.priority = %(priority)s
  ORDER BY priorities.idx, camera_idx
  LIMIT %(limit)s
)
GROUP BY camera_idx
;