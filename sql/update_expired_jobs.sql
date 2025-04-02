UPDATE jobs
SET job_status = 4,
    last_updated = NOW()
WHERE job_id = %(job_id)s
;