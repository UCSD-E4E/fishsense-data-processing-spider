WITH label_studio_images AS (
  SELECT image_md5, laser_task_id
  FROM images
  WHERE laser_task_id IS NOT NULL
),
priority_images AS (
  SELECT data_paths.path || images.path as path, images.camera_sn
  FROM images
  LEFT JOIN canonical_dives ON images.dive = canonical_dives.path
  LEFT JOIN priorities ON canonical_dives.priority = priorities.name
  LEFT JOIN label_studio_images ON images.image_md5 = label_studio_images.image_md5
  LEFT JOIN data_paths ON images.data_path = data_paths.idx
  WHERE images.laser_task_id IS NULL
  ORDER BY priorities.idx
  LIMIT 8000
),
grouped_images AS (
  SELECT array_agg(path) as paths, camera_sn
  FROM priority_images
  GROUP BY camera_sn
),
parameters AS (
  SELECT cameras.name, cameras.lens_cal_path, array_to_json(grouped_images.paths) as data
  FROM grouped_images
  LEFT JOIN cameras ON grouped_images.camera_sn = cameras.serial_number
)
SELECT json_object(
  'jobs': jobs
)
FROM (
  SELECT json_arrayagg(jobs) AS jobs
  FROM (
    SELECT json_object(
      'display_name': name,
      'job_name': 'preprocess',
      'parameters': params
    ) as job
    FROM (
      SELECT name, json_object (
        'data': data,
        'lens-calibration': lens_cal_path,
        'format': 'JPG',
        'output': '/home/ubuntu/label_studio_laser/2025-03-16'
      ) as params
      FROM parameters
    )
  ) as jobs
)