SELECT canonical_dives.path, data_paths.path as data_path
FROM canonical_dives
LEFT JOIN data_paths ON canonical_dives.data_path = data_paths.idx
ORDER BY path
;