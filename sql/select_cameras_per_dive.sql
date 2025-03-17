SELECT DISTINCT cameras.idx
FROM images
LEFT JOIN cameras ON images.camera_sn = cameras.serial_number
WHERE dive = %(dive)s AND NOT ignore
;