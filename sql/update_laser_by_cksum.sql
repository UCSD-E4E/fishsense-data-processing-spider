UPDATE laser_labels
SET x = %(x)s,
    y = %(y)s
WHERE cksum = %(cksum)s
;