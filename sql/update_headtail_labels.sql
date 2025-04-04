UPDATE headtail_labels 
SET head_x = %(head_x)s,
    head_y = %(head_y)s,
    tail_x = %(tail_x)s,
    tail_y = %(tail_y)s,
    complete = TRUE
WHERE cksum = %(cksum)s
;