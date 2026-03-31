CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_users_email ON users (email);

CREATE TABLE groups (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    perms INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE groups_users (
    group_id INTEGER NOT NULL REFERENCES groups(id),
    user_id INTEGER NOT NULL REFERENCES users(id),
    PRIMARY KEY (group_id, user_id)
);

CREATE VIEW group_members AS
SELECT g.name AS group_name, u.name AS user_name
FROM groups g
JOIN groups_users gu ON g.id = gu.group_id
JOIN users u ON gu.user_id = u.id;
