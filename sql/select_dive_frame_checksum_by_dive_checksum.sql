SELECT images.image_md5 as frames
FROM images
RIGHT JOIN canonical_dives ON images.dive = canonical_dives.path
WHERE canonical_dives.checksum = %(cksum)s
;