SET CHARACTER_SET_CLIENT = utf8mb4;
SET CHARACTER_SET_CONNECTION = utf8mb4;

drop database if exists isuride;
CREATE DATABASE IF NOT EXISTS isuride DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

DROP USER IF EXISTS 'isucon'@'%';
CREATE USER IF NOT EXISTS 'isucon'@'%' IDENTIFIED BY 'isucon';
GRANT ALL ON isuride.* TO 'isucon'@'%';
