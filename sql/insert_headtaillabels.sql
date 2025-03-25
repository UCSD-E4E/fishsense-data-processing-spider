INSERT INTO headtail_labels (cksum, task_id)
VALUES (%(cksum)s, %(task_id)s)
ON CONFLICT DO NOTHING;