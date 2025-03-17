WITH label_studio_images AS (
  SELECT image_md5, laser_task_id
  FROM images
  WHERE laser_task_id IS NOT NULL
),
priority_images AS (
  SELECT replace(data_paths.path || images.path, '/mnt/fishsense_data_reef/REEF/data', '/home/ubuntu/data') as path, images.camera_sn
  FROM images
  INNER JOIN canonical_dives ON images.dive = canonical_dives.path
  LEFT JOIN priorities ON canonical_dives.priority = priorities.name
  LEFT JOIN label_studio_images ON images.image_md5 = label_studio_images.image_md5
  LEFT JOIN data_paths ON images.data_path = data_paths.idx
  WHERE images.laser_task_id IS NULL AND label_studio_images.laser_task_id IS NULL
  ORDER BY priorities.idx
  LIMIT %(limit)s
),
grouped_images AS (
  SELECT array_agg(path) as paths, camera_sn
  FROM priority_images
  GROUP BY camera_sn
),
parameters AS (
  SELECT cameras.name, replace(cameras.lens_cal_path, '//e4e-nas.ucsd.edu/fishsense/Fishsense Lite Calibration Parameters', '/home/ubuntu/lens_cal') as lens_cal_path, array_to_json(grouped_images.paths) as data
  FROM grouped_images
  LEFT JOIN cameras ON grouped_images.camera_sn = cameras.serial_number
)
SELECT json_object(
  'jobs': jobs
)
FROM (
  SELECT json_arrayagg(json_object(
    'display_name': name,
    'job_name': 'preprocess',
    'parameters': params
  )) as jobs
  FROM (
    SELECT name, json_object (
      'data': data,
      'lens-calibration': lens_cal_path,
      'format': 'JPG',
      'output': %(output_dir)s
    ) as params
    FROM parameters
  )
)