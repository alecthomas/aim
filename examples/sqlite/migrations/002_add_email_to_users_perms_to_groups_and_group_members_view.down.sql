PRAGMA defer_foreign_keys = true;
DROP VIEW group_members;

ALTER TABLE groups DROP COLUMN perms;

DROP INDEX idx_users_email;

ALTER TABLE users DROP COLUMN email;
