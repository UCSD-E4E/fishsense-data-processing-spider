UPDATE images
SET camera_sn = %(camera_sn)s
WHERE image_md5 = %(cksum)s
;