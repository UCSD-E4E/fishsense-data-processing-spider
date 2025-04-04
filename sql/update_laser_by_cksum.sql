UPDATE laser_labels
SET x = %(x)s,
    y = %(y)s,
    complete = TRUE
WHERE cksum = %(cksum)s
;