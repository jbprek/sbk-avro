-- Batch  database
create database deaths;
CREATE USER 'deaths'@'%' IDENTIFIED BY 'deaths';
GRANT ALL PRIVILEGES ON deaths.* TO 'deaths'@'%';

CREATE TABLE IF NOT EXISTS deaths.male_deaths
(
    reg_id BIGINT NOT NULL PRIMARY KEY,
    name VARCHAR(100),
    dod DATE,
    town VARCHAR(50),
    UNIQUE KEY unique_name_dob (name, dod, town)

) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS deaths.female_deaths
(
    reg_id BIGINT NOT NULL PRIMARY KEY,
    name VARCHAR(100),
    dod DATE,
    town VARCHAR(50),
    UNIQUE KEY unique_name_dob (name, dod, town)

    ) ENGINE = InnoDB;