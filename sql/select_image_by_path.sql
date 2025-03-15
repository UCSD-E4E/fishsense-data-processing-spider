SELECT path, dive, camera_sn, image_md5, laser_task_id
FROM images
WHERE images.path = %(path)s
LIMIT 1
;