CREATE DATABASE IF NOT EXISTS birth_analytics_db;
CREATE USER IF NOT EXISTS 'birth_analytics'@'%' IDENTIFIED BY 'birth_analytics';
GRANT ALL PRIVILEGES ON birth_analytics_db.* TO 'birth_analytics'@'%';

USE birth_analytics_db;

CREATE TABLE IF NOT EXISTS birth_stats (
    reg_id BIGINT PRIMARY KEY,
    dob DATE NOT NULL,
    town VARCHAR(50) NOT NULL,
    gender  CHAR(1) NOT NULL,
    CONSTRAINT chk_births_gender CHECK (gender IN ('M', 'F')),
    UNIQUE KEY unique_name_dob (reg_id, dob, town)
) ENGINE=InnoDB;