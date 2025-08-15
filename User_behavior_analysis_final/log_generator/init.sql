CREATE TABLE IF NOT EXISTS user_logs (
    id SERIAL PRIMARY KEY,
    user_id TEXT,
    page TEXT,
    action TEXT,
    timestamp TIMESTAMPTZ
);
