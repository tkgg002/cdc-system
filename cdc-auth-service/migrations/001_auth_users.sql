-- Auth Service: Users table
-- Chạy trên database goopay_dw (shared với CDC tables)

BEGIN;

CREATE TABLE IF NOT EXISTS auth_users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(100) NOT NULL UNIQUE,
    email VARCHAR(200) NOT NULL UNIQUE,
    password VARCHAR(255) NOT NULL,  -- bcrypt hash
    full_name VARCHAR(200),
    role VARCHAR(20) NOT NULL DEFAULT 'operator',  -- 'admin', 'operator'
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),

    CONSTRAINT au_check_role CHECK (role IN ('admin', 'operator'))
);

CREATE INDEX IF NOT EXISTS idx_auth_users_username ON auth_users(username);
CREATE INDEX IF NOT EXISTS idx_auth_users_role ON auth_users(role);

-- Seed: default admin user (password: admin123 — bcrypt hash)
-- Thay đổi password sau khi deploy
INSERT INTO auth_users (username, email, password, full_name, role)
VALUES (
    'admin',
    'admin@goopay.vn',
    '$2a$10$0koc2s0krtdFu5L62ltWzOtnBk0b.DFbcgJHjLl4.jXntdhFUd60y', -- admin123
    'System Admin',
    'admin'
) ON CONFLICT (username) DO NOTHING;

COMMIT;
