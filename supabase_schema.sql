-- ============================================
-- TLE Discord Bot - Supabase Schema
-- Run this in Supabase SQL Editor
-- ============================================

-- User handles (Discord ID to CF handle mapping)
CREATE TABLE IF NOT EXISTS user_handle (
    user_id TEXT NOT NULL,
    guild_id TEXT NOT NULL,
    handle TEXT NOT NULL,
    active BOOLEAN DEFAULT true,
    PRIMARY KEY (user_id, guild_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS ix_user_handle_guild_handle ON user_handle (guild_id, handle);

-- CF user cache
CREATE TABLE IF NOT EXISTS cf_user_cache (
    handle TEXT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    country TEXT,
    city TEXT,
    organization TEXT,
    contribution INTEGER,
    rating INTEGER,
    max_rating INTEGER,
    last_online_time BIGINT,
    registration_time BIGINT,
    friend_of_count INTEGER,
    title_photo TEXT
);

-- Duelist ratings
CREATE TABLE IF NOT EXISTS duelist (
    user_id BIGINT PRIMARY KEY,
    rating INTEGER NOT NULL DEFAULT 1500
);

-- 1v1 Duels
CREATE TABLE IF NOT EXISTS duel (
    id SERIAL PRIMARY KEY,
    challenger BIGINT NOT NULL,
    challengee BIGINT NOT NULL,
    issue_time DOUBLE PRECISION NOT NULL,
    start_time DOUBLE PRECISION,
    finish_time DOUBLE PRECISION,
    problem_name TEXT,
    contest_id INTEGER,
    p_index TEXT,
    status INTEGER,
    winner INTEGER,
    type INTEGER,
    nohandicap INTEGER DEFAULT 0
);

-- Challenges (gitgud)
CREATE TABLE IF NOT EXISTS challenge (
    id SERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    issue_time DOUBLE PRECISION NOT NULL,
    finish_time DOUBLE PRECISION,
    problem_name TEXT NOT NULL,
    contest_id INTEGER NOT NULL,
    p_index TEXT NOT NULL,
    rating_delta INTEGER NOT NULL,
    status INTEGER NOT NULL
);

-- User challenge state
CREATE TABLE IF NOT EXISTS user_challenge (
    user_id TEXT PRIMARY KEY,
    active_challenge_id INTEGER,
    issue_time DOUBLE PRECISION,
    score INTEGER NOT NULL DEFAULT 0,
    num_completed INTEGER NOT NULL DEFAULT 0,
    num_skipped INTEGER NOT NULL DEFAULT 0
);

-- Reminders
CREATE TABLE IF NOT EXISTS reminder (
    guild_id TEXT PRIMARY KEY,
    channel_id TEXT,
    role_id TEXT,
    before TEXT
);

-- Rankup channel
CREATE TABLE IF NOT EXISTS rankup (
    guild_id TEXT PRIMARY KEY,
    channel_id TEXT
);

-- Auto role update
CREATE TABLE IF NOT EXISTS auto_role_update (
    guild_id TEXT PRIMARY KEY
);

-- Rated VCs
CREATE TABLE IF NOT EXISTS rated_vcs (
    id SERIAL PRIMARY KEY,
    contest_id INTEGER NOT NULL,
    start_time DOUBLE PRECISION,
    finish_time DOUBLE PRECISION,
    status INTEGER,
    guild_id TEXT
);

CREATE TABLE IF NOT EXISTS rated_vc_users (
    vc_id INTEGER NOT NULL REFERENCES rated_vcs(id),
    user_id TEXT NOT NULL,
    rating INTEGER,
    PRIMARY KEY (vc_id, user_id)
);

CREATE TABLE IF NOT EXISTS rated_vc_settings (
    guild_id TEXT PRIMARY KEY,
    channel_id TEXT
);

-- Starboard tables
CREATE TABLE IF NOT EXISTS starboard_config_v1 (
    guild_id TEXT,
    emoji TEXT,
    channel_id TEXT,
    PRIMARY KEY (guild_id, emoji)
);

CREATE TABLE IF NOT EXISTS starboard_emoji_v1 (
    guild_id TEXT,
    emoji TEXT,
    threshold INTEGER,
    color INTEGER,
    PRIMARY KEY (guild_id, emoji)
);

CREATE TABLE IF NOT EXISTS starboard_message_v1 (
    original_msg_id TEXT,
    starboard_msg_id TEXT,
    guild_id TEXT,
    emoji TEXT,
    PRIMARY KEY (original_msg_id, emoji)
);

-- Multi-player duels
CREATE TABLE IF NOT EXISTS multiplayer_duel (
    id SERIAL PRIMARY KEY,
    creator_id BIGINT NOT NULL,
    guild_id TEXT NOT NULL,
    issue_time DOUBLE PRECISION NOT NULL,
    start_time DOUBLE PRECISION,
    finish_time DOUBLE PRECISION,
    status INTEGER,
    type INTEGER,
    num_problems INTEGER NOT NULL,
    rating INTEGER,
    nohandicap INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS multiplayer_duel_participant (
    duel_id INTEGER NOT NULL REFERENCES multiplayer_duel(id),
    user_id BIGINT NOT NULL,
    status INTEGER,
    problems_solved INTEGER DEFAULT 0,
    total_time DOUBLE PRECISION DEFAULT 0,
    placement INTEGER,
    rating_delta INTEGER,
    PRIMARY KEY (duel_id, user_id)
);

CREATE TABLE IF NOT EXISTS multiplayer_duel_problem (
    duel_id INTEGER NOT NULL REFERENCES multiplayer_duel(id),
    problem_name TEXT NOT NULL,
    contest_id INTEGER NOT NULL,
    p_index TEXT NOT NULL,
    problem_order INTEGER NOT NULL,
    PRIMARY KEY (duel_id, problem_order)
);

-- ============================================
-- Row Level Security (Optional but recommended)
-- ============================================
-- Enable RLS on all tables
ALTER TABLE user_handle ENABLE ROW LEVEL SECURITY;
ALTER TABLE duelist ENABLE ROW LEVEL SECURITY;
ALTER TABLE duel ENABLE ROW LEVEL SECURITY;

-- Allow full access with service role key (used by bot)
CREATE POLICY "Service role full access" ON user_handle FOR ALL USING (true);
CREATE POLICY "Service role full access" ON duelist FOR ALL USING (true);
CREATE POLICY "Service role full access" ON duel FOR ALL USING (true);
CREATE POLICY "Service role full access" ON challenge FOR ALL USING (true);
CREATE POLICY "Service role full access" ON user_challenge FOR ALL USING (true);
CREATE POLICY "Service role full access" ON reminder FOR ALL USING (true);
CREATE POLICY "Service role full access" ON rankup FOR ALL USING (true);
CREATE POLICY "Service role full access" ON auto_role_update FOR ALL USING (true);
CREATE POLICY "Service role full access" ON rated_vcs FOR ALL USING (true);
CREATE POLICY "Service role full access" ON rated_vc_users FOR ALL USING (true);
CREATE POLICY "Service role full access" ON rated_vc_settings FOR ALL USING (true);
CREATE POLICY "Service role full access" ON starboard_config_v1 FOR ALL USING (true);
CREATE POLICY "Service role full access" ON starboard_emoji_v1 FOR ALL USING (true);
CREATE POLICY "Service role full access" ON starboard_message_v1 FOR ALL USING (true);
CREATE POLICY "Service role full access" ON multiplayer_duel FOR ALL USING (true);
CREATE POLICY "Service role full access" ON multiplayer_duel_participant FOR ALL USING (true);
CREATE POLICY "Service role full access" ON multiplayer_duel_problem FOR ALL USING (true);
CREATE POLICY "Service role full access" ON cf_user_cache FOR ALL USING (true);
