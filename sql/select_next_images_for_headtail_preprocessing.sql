WITH priority_images AS (
  SELECT data_paths.path || images.path as paths, images.camera_sn
  FROM images
  INNER JOIN canonical_dives ON images.dive = canonical_dives.path
  RIGHT JOIN laser_labels ON images.image_md5 = laser_labels.cksum
  LEFT JOIN priorities ON canonical_dives.priority = priorities.name
  LEFT JOIN data_paths ON images.data_path = data_paths.idx
  LEFT JOIN headtail_labels ON images.image_md5 = headtail_labels.cksum
  WHERE laser_labels.x IS NOT NULL AND headtail_labels.task_id IS NULL
  ORDER BY priorities.idx
  LIMIT %(limit)s
),
grouped_images AS (
  SELECT array_agg(paths) as paths, camera_sn
  FROM priority_images
  GROUP BY camera_sn
),
params AS (
  SELECT cameras.lens_cal_path, cameras.name, paths
  FROM grouped_images
  LEFT JOIN cameras ON grouped_images.camera_sn = cameras.serial_number
)
SELECT 
json_object(
  'jobs': json_arrayagg(
    json_object(
      'display_name': name,
      'job_name': 'preprocess_with_laser',
      'parameters': json_object(
        'data': paths,
        'lens-calibration': lens_cal_path,
        'format': 'JPG',
        'output': %(output_dir)s
      )
    )
  )
)
FROM params