UPDATE images
SET
    preprocess_laser_job_id = NULL
WHERE
    preprocess_laser_job_id = %(job_id)s
;