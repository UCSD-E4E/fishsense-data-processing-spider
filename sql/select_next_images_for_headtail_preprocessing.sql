WITH label_studio_tasks AS (
  SELECT image_md5, headtail_task_id
  FROM images
  WHERE headtail_task_id IS NOT NULL
),
priority_images AS (
  SELECT data_paths.path || images.path as path, images.camera_sn, images.laser_task_id
  FROM images
  INNER JOIN canonical_dives on images.dive = canonical_dives.path
  LEFT JOIN priorities ON canonical_dives.priority = priorities.name
  LEFT JOIN label_studio_tasks ON images.image_md5 = label_studio_tasks.image_md5
  LEFT JOIN data_paths ON images.data_path = data_paths.idx
  WHERE images.headtail_task_id IS NULL AND label_studio_tasks.headtail_task_id IS NULL
  ORDER BY priorities.idx
)

LIMIT 16
