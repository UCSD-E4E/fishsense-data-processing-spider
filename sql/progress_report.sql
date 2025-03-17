SELECT path, date, site, priority, cameras.name || 'D' as camera, dive_slates.name
FROM canonical_dives
LEFT JOIN cameras ON canonical_dives.camera = cameras.idx
LEFT JOIN dive_slates ON canonical_dives.slate_id = dive_slates.idx
ORDER BY path
;