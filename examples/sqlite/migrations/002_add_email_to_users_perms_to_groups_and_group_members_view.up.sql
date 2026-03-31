PRAGMA defer_foreign_keys = true;
ALTER TABLE users ADD COLUMN email TEXT NOT NULL DEFAULT '';

CREATE INDEX idx_users_email ON users(email);

ALTER TABLE groups ADD COLUMN perms INTEGER NOT NULL;

CREATE VIEW group_members AS
SELECT g.name AS group_name, u.name AS user_name
FROM groups g
JOIN groups_users gu ON g.id = gu.group_id
JOIN users u ON gu.user_id = u.id;
