WITH expired_jobs AS (
SELECT job_id
FROM jobs
WHERE expiration < NOW() AND job_type = 'preprocess' AND job_status = 0
)
UPDATE images
SET preprocess_job_id = NULL
FROM expired_jobs
WHERE images.preprocess_job_id = expired_jobs.job_id
RETURNING images.image_md5, expired_jobs.job_id
;