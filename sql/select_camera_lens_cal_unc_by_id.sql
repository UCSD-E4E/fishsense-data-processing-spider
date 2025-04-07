SELECT cameras.lens_cal_path as path
FROM cameras
WHERE cameras.idx = %(camera_id)s
;