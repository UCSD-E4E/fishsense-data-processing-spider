SELECT path, image_md5
FROM images
WHERE dive = %(dive)s
ORDER BY path
;