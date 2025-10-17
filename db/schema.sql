CREATE DATABASE IF NOT EXISTS nyc311;
USE nyc311;
CREATE TABLE IF NOT EXISTS service_requests (
  unique_key BIGINT PRIMARY KEY,
  created_date DATETIME NOT NULL,
  closed_date DATETIME NULL,
  agency VARCHAR(16),
  complaint_type VARCHAR(128),
  descriptor VARCHAR(255),
  borough VARCHAR(32),
  latitude DECIMAL(9,6),
  longitude DECIMAL(9,6)
);