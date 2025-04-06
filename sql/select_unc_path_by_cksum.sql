SELECT data_paths.unc_path || images.path as path
FROM images
INNER JOIN canonical_dives ON images.dive = canonical_dives.path
LEFT JOIN data_paths ON data_paths.idx = images.data_path
WHERE
  images.image_md5 = %(cksum)s AND
  images.ignore = false
;