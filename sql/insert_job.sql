INSERT INTO jobs (job_id, worker, job_type, expiration, origin)
VALUES (
    %(job_id)s,
    %(worker)s,
    %(job_type)s,
    %(expiration)s,
    %(origin)s
)
;