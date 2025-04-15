SELECT images.path as image_path, images.dive, cameras.idx as camera_idx, images.date
FROM images
LEFT JOIN cameras ON images.camera_sn = cameras.serial_number
WHERE image_md5 = %(cksum)s
ORDER BY images.path
LIMIT 1;
;