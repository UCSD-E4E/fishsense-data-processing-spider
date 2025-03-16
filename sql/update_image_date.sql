UPDATE images
SET "date" = %(date)s
WHERE image_md5 = %(cksum)s
;