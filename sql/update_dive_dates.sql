UPDATE dives
SET date = %(date)s,
    invalid_image = %(invalid_image)s,
    multiple_date = %(multiple_date)s
WHERE dives.path = %(path)
;