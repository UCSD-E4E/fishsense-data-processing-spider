UPDATE images
SET
    preprocess_job_id = NULL
WHERE
    preprocess_job_id = %(job_id)s
;