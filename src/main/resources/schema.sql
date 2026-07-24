-- Storyboard risk incremental analysis service schema
-- Event sourcing: append-only event log + transactional outbox + read projections.

-- Append-only domain event log. Each event carries a per-storyboard monotonic version.
CREATE TABLE IF NOT EXISTS storyboard_event (
    id              BIGSERIAL PRIMARY KEY,
    event_id        UUID        NOT NULL UNIQUE,
    storyboard_id   VARCHAR(64) NOT NULL,
    version         BIGINT      NOT NULL,
    event_type      VARCHAR(32) NOT NULL,
    payload         JSONB       NOT NULL,
    created_at      TIMESTAMP   NOT NULL DEFAULT now(),
    UNIQUE (storyboard_id, version)
);
CREATE INDEX IF NOT EXISTS idx_event_storyboard ON storyboard_event (storyboard_id, version);

-- Transactional outbox: written in the same tx as the event, drained by the publisher.
CREATE TABLE IF NOT EXISTS outbox (
    id              BIGSERIAL PRIMARY KEY,
    event_id        UUID        NOT NULL UNIQUE,
    storyboard_id   VARCHAR(64) NOT NULL,
    version         BIGINT      NOT NULL,
    topic           VARCHAR(128) NOT NULL,
    payload         JSONB       NOT NULL,
    published       BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMP   NOT NULL DEFAULT now(),
    published_at    TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_outbox_unpublished ON outbox (published, id) WHERE published = FALSE;

-- Current version per storyboard used for optimistic locking on writes.
CREATE TABLE IF NOT EXISTS storyboard_head (
    storyboard_id   VARCHAR(64) PRIMARY KEY,
    version         BIGINT      NOT NULL,
    revision_kp     JSONB,
    original_kp     JSONB,
    updated_at      TIMESTAMP   NOT NULL DEFAULT now()
);

-- Segment projection: continuous, non-overlapping timeline rebuilt from events.
CREATE TABLE IF NOT EXISTS segment_projection (
    storyboard_id   VARCHAR(64) NOT NULL,
    segment_id      VARCHAR(64) NOT NULL,
    order_index     INT         NOT NULL,
    start_ms        BIGINT      NOT NULL,
    duration_ms     BIGINT      NOT NULL,
    intensity       INT         NOT NULL,
    is_reversal     BOOLEAN     NOT NULL DEFAULT FALSE,
    is_decline_inducement BOOLEAN NOT NULL DEFAULT FALSE,
    knowledge_point VARCHAR(128),
    projection_version BIGINT   NOT NULL,
    PRIMARY KEY (storyboard_id, segment_id)
);
CREATE INDEX IF NOT EXISTS idx_segment_order ON segment_projection (storyboard_id, order_index);

-- Latest analysis result projection.
CREATE TABLE IF NOT EXISTS analysis_projection (
    storyboard_id   VARCHAR(64) PRIMARY KEY,
    projection_version BIGINT   NOT NULL,
    rule_version    VARCHAR(32) NOT NULL,
    findings        JSONB       NOT NULL,
    analyzed_at     TIMESTAMP   NOT NULL DEFAULT now()
);

-- Consumer offset / last-processed version per storyboard for idempotency.
CREATE TABLE IF NOT EXISTS processed_version (
    storyboard_id   VARCHAR(64) PRIMARY KEY,
    last_version    BIGINT      NOT NULL,
    updated_at      TIMESTAMP   NOT NULL DEFAULT now()
);

-- Idempotency ledger for already-consumed event ids.
CREATE TABLE IF NOT EXISTS processed_event (
    event_id        UUID        PRIMARY KEY,
    processed_at    TIMESTAMP   NOT NULL DEFAULT now()
);

-- Dead letter queue for events that repeatedly fail analysis.
CREATE TABLE IF NOT EXISTS dead_letter (
    id              BIGSERIAL PRIMARY KEY,
    event_id        UUID        NOT NULL,
    storyboard_id   VARCHAR(64) NOT NULL,
    version         BIGINT      NOT NULL,
    payload         JSONB       NOT NULL,
    error           TEXT        NOT NULL,
    attempts        INT         NOT NULL,
    replayed        BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMP   NOT NULL DEFAULT now()
);

-- Streaming import jobs with cancel/resume checkpoints.
CREATE TABLE IF NOT EXISTS import_job (
    job_id          UUID        PRIMARY KEY,
    storyboard_id   VARCHAR(64) NOT NULL,
    status          VARCHAR(16) NOT NULL,
    total_segments  BIGINT      NOT NULL DEFAULT 0,
    processed_segments BIGINT   NOT NULL DEFAULT 0,
    last_offset     BIGINT      NOT NULL DEFAULT 0,
    cancel_requested BOOLEAN    NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMP   NOT NULL DEFAULT now(),
    updated_at      TIMESTAMP   NOT NULL DEFAULT now()
);
