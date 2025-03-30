UPDATE images
SET preprocess_job_id = %(job_id)s
WHERE images.image_md5 = %(checksum)s
;