SELECT images.image_md5 as cksum, images.path as path, data_paths.path as data_path
FROM images
LEFT JOIN data_paths ON images.data_path = data_paths.idx
WHERE images.camera_sn IS NULL AND NOT images.ignore
LIMIT %(limit)s
;