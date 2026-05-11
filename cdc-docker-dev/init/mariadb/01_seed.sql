-- CREATE DATABASE IF NOT EXISTS goopay_legacy_maria;
-- USE goopay_legacy_maria;

-- CREATE TABLE IF NOT EXISTS legacy_orders (
--     id          BIGINT PRIMARY KEY AUTO_INCREMENT,
--     order_code  VARCHAR(64) NOT NULL,
--     user_id     BIGINT NOT NULL,
--     amount      INT NOT NULL,
--     status      VARCHAR(32) NOT NULL DEFAULT 'pending',
--     created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
--     updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
-- ) ENGINE=InnoDB;

-- INSERT INTO legacy_orders (order_code, user_id, amount, status) VALUES
--     ('LEG-0001', 1001, 12000, 'pending'),
--     ('LEG-0002', 1002, 25000, 'pending'),
--     ('LEG-0003', 1003, 18500, 'paid'),
--     ('LEG-0004', 1004, 41000, 'paid'),
--     ('LEG-0005', 1005,  9900, 'cancelled');

-- GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT
--     ON *.* TO 'cdc'@'%';
-- GRANT ALL PRIVILEGES ON goopay_legacy_maria.* TO 'cdc'@'%';
-- FLUSH PRIVILEGES;
