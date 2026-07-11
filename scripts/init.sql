-- DAGForge MySQL initialization script
-- Creates database, user, and grants privileges

-- Create database
CREATE DATABASE IF NOT EXISTS dagforge CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Create users for container and local development connections
CREATE USER IF NOT EXISTS 'dagforge'@'%' IDENTIFIED BY 'dagforge';
CREATE USER IF NOT EXISTS 'dagforge'@'localhost' IDENTIFIED BY 'dagforge';

-- Grant production database access
GRANT ALL PRIVILEGES ON dagforge.* TO 'dagforge'@'%';
GRANT ALL PRIVILEGES ON dagforge.* TO 'dagforge'@'localhost';

-- Tests create isolated databases named dagforge_test_<pid>.
GRANT ALL PRIVILEGES ON `dagforge\_test\_%`.* TO 'dagforge'@'%';
GRANT ALL PRIVILEGES ON `dagforge\_test\_%`.* TO 'dagforge'@'localhost';

-- Apply changes
FLUSH PRIVILEGES;
