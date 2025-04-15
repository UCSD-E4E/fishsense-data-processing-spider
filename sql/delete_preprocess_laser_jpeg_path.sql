UPDATE images
SET preprocess_laser_jpeg_path = NULL
WHERE image_md5 = %(cksum)s
;