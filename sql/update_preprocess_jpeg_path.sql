UPDATE images
SET preprocess_jpeg_path = %(unc_path)
WHERE image_md5 = %(cksum)s
;