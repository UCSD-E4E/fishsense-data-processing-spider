UPDATE laser_labels
SET x = %(x)s,
    y = %(y)s
WHERE task_id = %(task_id)s
;