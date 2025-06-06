CREATE TABLE IF NOT EXISTS user_logs (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(50),
    page VARCHAR(100),
    action VARCHAR(50),
    timestamp TIMESTAMP
);
