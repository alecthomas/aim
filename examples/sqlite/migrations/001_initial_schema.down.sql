-- migraitor: timestamp=2026-03-31T07:18:38Z engine=sqlite checksum=37124bf14fd02ed4da82572db58c172d16dc753ff42b2d9be71fc434229e3a7e
PRAGMA foreign_keys = OFF;
DROP TABLE groups_users;
DROP TABLE groups;
DROP TABLE users;
PRAGMA foreign_keys = ON;
