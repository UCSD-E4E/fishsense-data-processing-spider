UPDATE images
SET preprocess_laser_jpeg_path = %(unc_path)s
WHERE image_md5 = %(cksum)s
;