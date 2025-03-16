SELECT DISTINCT (to_char(images.date, 'YYYY-MM-DD')) as date
FROM images
WHERE dive = %(dive)s AND NOT images.ignore
;