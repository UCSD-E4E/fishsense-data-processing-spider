SELECT camera_idx, array_agg(image_md5) as checksums
FROM (
  SELECT images.image_md5, cameras.idx as camera_idx
  FROM images
  RIGHT JOIN canonical_dives ON images.dive = canonical_dives.path
  LEFT JOIN priorities ON canonical_dives.priority = priorities.name
  LEFT JOIN laser_labels ON images.image_md5 = laser_labels.cksum
  LEFT JOIN cameras ON images.camera_sn = cameras.serial_number
  WHERE laser_labels.task_id IS NULL AND
    images.preprocess_job_id IS NULL AND
    canonical_dives.priority = %(priority)s
  ORDER BY priorities.idx, camera_idx
  LIMIT %(limit)s
)
GROUP BY camera_idx
;