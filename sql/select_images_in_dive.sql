SELECT path, image_md5
FROM images
WHERE dive = %(dive)s AND NOT images.ignore
ORDER BY path
;