CREATE TABLE data_paths (
    "idx" INT PRIMARY KEY,
    "path" TEXT NOT NULL
);

INSERT INTO data_paths (idx, path)
VALUES (0, '/mnt/fishsense_data_reef/REEF/data/');

ALTER TABLE images
ADD data_path INT REFERENCES data_paths;

UPDATE images
SET data_path = 0;

CREATE INDEX "images_image_md5" ON "images" ("image_md5");