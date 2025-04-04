UPDATE laser_labels
SET x = %(x)s,
    y = %(y)s,
    complete = TRUE
WHERE task_id = %(task_id)s
;