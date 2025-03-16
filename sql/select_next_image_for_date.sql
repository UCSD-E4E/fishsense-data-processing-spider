SELECT images.image_md5 as cksum, regexp_replace(data_paths.path || images.path, '\.ORF$', '.JPG') as img_path
FROM images
LEFT JOIN data_paths ON images.data_path = data_paths.idx
WHERE date IS NULL AND NOT images.ignore
LIMIT %(limit)s
;