ALTER TABLE canonical_dives
ADD COLUMN slate_images TEXT NULL
CONSTRAINT canonical_dives_slate_images_fk_canonical_dives_path REFERENCES canonical_dives (path);