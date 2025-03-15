INSERT INTO images (
    path,
    dive,
    image_md5
) VALUES (
    %(path)s,
    %(dive)s,
    %(cksum)s
);