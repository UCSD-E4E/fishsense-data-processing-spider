UPDATE jobs
SET
    job_status = %(job_status)s,
    last_updated = NOW()
WHERE
    job_id = %(job_id)s
;