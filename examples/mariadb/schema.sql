CREATE TABLE users (
    id INT NOT NULL AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL DEFAULT '',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
) ENGINE=InnoDB;

CREATE INDEX idx_users_email ON users (email);

CREATE TABLE `groups` (
    id INT NOT NULL AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL,
    perms INT NOT NULL DEFAULT 0,
    PRIMARY KEY (id)
) ENGINE=InnoDB;

CREATE TABLE groups_users (
    group_id INT NOT NULL,
    user_id INT NOT NULL,
    PRIMARY KEY (group_id, user_id),
    FOREIGN KEY (group_id) REFERENCES `groups`(id),
    FOREIGN KEY (user_id) REFERENCES users(id)
) ENGINE=InnoDB;

CREATE VIEW group_members AS
SELECT g.name AS group_name, u.name AS user_name
FROM `groups` g
JOIN groups_users gu ON g.id = gu.group_id
JOIN users u ON gu.user_id = u.id;
