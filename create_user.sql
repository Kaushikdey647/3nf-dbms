CREATE USER 'test_user'@'localhost' IDENTIFIED WITH mysql_native_password BY 'Aauth123';
GRANT ALL PRIVILEGES ON employees.* TO 'test_user'@'localhost';
USE employees;