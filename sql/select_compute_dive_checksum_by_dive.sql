SELECT MD5(STRING_AGG(path, ''))
FROM (
    SELECT SPLIT_PART(path, '/', -1) || ':' || image_md5 || E'\n' as path
    FROM images
    WHERE dive = %(dive)s
    ORDER BY path
)
;