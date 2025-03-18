CREATE TABLE laser_labels (
    cksum TEXT PRIMARY KEY,
    x INT,
    y INT
);

CREATE TABLE headtail_labels (
    cksum TEXT PRIMARY KEY,
    head_x INT,
    head_y INT,
    tail_x INT,
    tail_y INT
);