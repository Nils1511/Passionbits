-- competitor_ads remains as before
CREATE TABLE competitor_ads (
    id                SERIAL PRIMARY KEY,
    brand             TEXT            NOT NULL,
    input_url         TEXT,
    page_id           TEXT,
    page_name         TEXT,
    page_likes        INTEGER,
    ad_archive_id     TEXT,
    start_date        TIMESTAMP WITH TIME ZONE,
    end_date          TIMESTAMP WITH TIME ZONE,
    is_active         BOOLEAN,
    total_active_time INTEGER,
    cta_text          TEXT,
    link_url          TEXT,
    snapshot_caption  TEXT,
    raw_json          JSONB            NOT NULL,
    inserted_at       TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- NEW: one row per card, references competitor_ads.id
CREATE TABLE ad_cards (
    id                   SERIAL PRIMARY KEY,
    ad_id                INTEGER   NOT NULL
                           REFERENCES competitor_ads(id)
                           ON DELETE CASCADE,
    body                 TEXT,
    caption              TEXT,
    cta_text             TEXT,
    cta_type             TEXT,
    link_description     TEXT,
    link_url             TEXT,
    title                TEXT,
    video_hd_url         TEXT,
    video_sd_url         TEXT,
    video_preview_image  TEXT,
    inserted_at          TIMESTAMP WITH TIME ZONE DEFAULT now()
);
-- competitor_reels: one row per reel
CREATE TABLE competitor_reels (
  id               SERIAL PRIMARY KEY,
  brand            TEXT NOT NULL,
  input_url        TEXT,
  reel_id          TEXT UNIQUE,
  shortcode        TEXT,
  caption          TEXT,
  url              TEXT,
  comments_count   INT,
  likes_count      INT,
  video_url        TEXT,
  display_url      TEXT,
  timestamp        TIMESTAMPTZ,
  raw_json         JSONB,
  inserted_at      TIMESTAMPTZ DEFAULT now()
);

-- reel_comments: one row per comment/reply
CREATE TABLE reel_comments (
  id               SERIAL PRIMARY KEY,
  reel_id          INTEGER NOT NULL REFERENCES competitor_reels(id),
  comment_id       TEXT,
  text             TEXT,
  owner_username   TEXT,
  owner_id         TEXT,
  timestamp        TIMESTAMPTZ,
  parent_comment_id TEXT,    -- for replies
  raw_json         JSONB,
  inserted_at      TIMESTAMPTZ DEFAULT now()
);
