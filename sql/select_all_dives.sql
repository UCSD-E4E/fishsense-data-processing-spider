SELECT dives.path, data_paths.path as data_path
FROM dives
LEFT JOIN data_paths ON dives.data_path = data_paths.idx
;
