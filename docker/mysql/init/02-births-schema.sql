-- Batch  database
create database births;
CREATE USER 'births'@'%' IDENTIFIED BY 'births';
GRANT ALL PRIVILEGES ON births.* TO 'births'@'%';

CREATE TABLE IF NOT EXISTS births.births
(
    reg_id BIGINT NOT NULL PRIMARY KEY,
    reg_time TIMESTAMP NOT NULL,
    name VARCHAR(100) NOT NULL,
    dob DATE NOT NULL,
    town VARCHAR(50) NOT NULL,
    weight  DECIMAL(3,1) NOT NULL,
    gender  CHAR(1) NOT NULL,
    CONSTRAINT chk_births_gender CHECK (gender IN ('M', 'F')),
    UNIQUE KEY unique_name_dob (name, dob, town)

) ENGINE = InnoDB;