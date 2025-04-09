SELECT task_id, x, y
FROM laser_labels
WHERE cksum = %(cksum)s AND
    complete = TRUE
;