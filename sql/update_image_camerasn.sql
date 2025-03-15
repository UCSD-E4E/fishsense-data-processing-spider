UPDATE images
SET images.camera_sn = %(camera_sn)s
WHERE images.image_md5 = %(cksum)s
;