CREATE TABLE IF NOT EXISTS event_log (
    event_id        UUID PRIMARY KEY,
    timeline_id     VARCHAR(128) NOT NULL,
    aggregate_id    VARCHAR(128) NOT NULL,
    event_type      VARCHAR(64)  NOT NULL,
    event_version   BIGINT       NOT NULL,
    payload         JSONB        NOT NULL,
    metadata        JSONB        NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (aggregate_id, event_version)
);

CREATE INDEX idx_event_log_timeline ON event_log(timeline_id, event_version);
CREATE INDEX idx_event_log_type ON event_log(event_type);
CREATE INDEX idx_event_log_created ON event_log(created_at);

CREATE TABLE IF NOT EXISTS timeline_projection (
    timeline_id     VARCHAR(128) PRIMARY KEY,
    current_version BIGINT       NOT NULL DEFAULT 0,
    total_segments  INT          NOT NULL DEFAULT 0,
    total_duration_ms BIGINT     NOT NULL DEFAULT 0,
    last_event_id   UUID,
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS segment_projection (
    segment_id       VARCHAR(128) NOT NULL,
    timeline_id      VARCHAR(128) NOT NULL,
    version          BIGINT       NOT NULL,
    order_index      INT          NOT NULL,
    start_time_ms    BIGINT       NOT NULL,
    end_time_ms      BIGINT       NOT NULL,
    intensity        INT          NOT NULL,
    is_reversal      BOOLEAN      NOT NULL DEFAULT false,
    is_knowledge_point BOOLEAN    NOT NULL DEFAULT false,
    is_decline_inducement BOOLEAN NOT NULL DEFAULT false,
    knowledge_point_id VARCHAR(128),
    is_original      BOOLEAN      NOT NULL DEFAULT true,
    content          TEXT,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (segment_id, timeline_id)
);

CREATE INDEX idx_segment_timeline_order ON segment_projection(timeline_id, order_index);
CREATE INDEX idx_segment_timeline_time ON segment_projection(timeline_id, start_time_ms);
CREATE INDEX idx_segment_kp ON segment_projection(timeline_id, knowledge_point_id) WHERE knowledge_point_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS risk_finding (
    finding_id      UUID PRIMARY KEY,
    timeline_id     VARCHAR(128) NOT NULL,
    rule_id         VARCHAR(64)  NOT NULL,
    rule_version    VARCHAR(32)  NOT NULL,
    severity        VARCHAR(16)  NOT NULL,
    segment_ids     JSONB        NOT NULL,
    time_range_ms   BIGINT[],
    evidence        JSONB        NOT NULL,
    suggestion      TEXT         NOT NULL,
    analysis_version BIGINT      NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (timeline_id, rule_id, analysis_version, finding_id)
);

CREATE INDEX idx_risk_finding_timeline ON risk_finding(timeline_id, analysis_version);
CREATE INDEX idx_risk_finding_rule ON risk_finding(rule_id);

CREATE TABLE IF NOT EXISTS outbox (
    id              BIGSERIAL PRIMARY KEY,
    event_id        UUID NOT NULL,
    aggregate_id    VARCHAR(128) NOT NULL,
    topic           VARCHAR(128) NOT NULL,
    key             VARCHAR(256) NOT NULL,
    payload         JSONB NOT NULL,
    headers         JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at    TIMESTAMPTZ,
    published       BOOLEAN NOT NULL DEFAULT false
);

CREATE INDEX idx_outbox_unpublished ON outbox(published, created_at) WHERE published = false;

CREATE TABLE IF NOT EXISTS consumer_offsets (
    consumer_group  VARCHAR(128) NOT NULL,
    topic           VARCHAR(128) NOT NULL,
    partition       INT NOT NULL,
    offset          BIGINT NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_group, topic, partition)
);

CREATE TABLE IF NOT EXISTS processed_events (
    consumer_group  VARCHAR(128) NOT NULL,
    event_id        UUID NOT NULL,
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    result          VARCHAR(32) NOT NULL,
    PRIMARY KEY (consumer_group, event_id)
);

CREATE TABLE IF NOT EXISTS dead_letter (
    id              BIGSERIAL PRIMARY KEY,
    event_id        UUID NOT NULL,
    consumer_group  VARCHAR(128) NOT NULL,
    topic           VARCHAR(128) NOT NULL,
    partition       INT NOT NULL,
    offset          BIGINT NOT NULL,
    payload         JSONB NOT NULL,
    error_message   TEXT NOT NULL,
    error_class     VARCHAR(256),
    attempt_count   INT NOT NULL DEFAULT 1,
    replayed        BOOLEAN NOT NULL DEFAULT false,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    replayed_at     TIMESTAMPTZ
);

CREATE INDEX idx_dlq_unreplayed ON dead_letter(replayed, created_at) WHERE replayed = false;

CREATE TABLE IF NOT EXISTS import_job (
    job_id          UUID PRIMARY KEY,
    timeline_id     VARCHAR(128) NOT NULL,
    total_count     BIGINT NOT NULL DEFAULT 0,
    processed_count BIGINT NOT NULL DEFAULT 0,
    failed_count    BIGINT NOT NULL DEFAULT 0,
    status          VARCHAR(32) NOT NULL DEFAULT 'pending',
    cancel_requested BOOLEAN NOT NULL DEFAULT false,
    resume_token    VARCHAR(512),
    error_message   TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at    TIMESTAMPTZ
);

CREATE INDEX idx_import_job_timeline ON import_job(timeline_id);
CREATE INDEX idx_import_job_status ON import_job(status);

CREATE TABLE IF NOT EXISTS import_chunk (
    id              BIGSERIAL PRIMARY KEY,
    job_id          UUID NOT NULL REFERENCES import_job(job_id),
    chunk_index     INT NOT NULL,
    segment_count   INT NOT NULL,
    payload         JSONB NOT NULL,
    status          VARCHAR(32) NOT NULL DEFAULT 'pending',
    processed_at    TIMESTAMPTZ,
    error_message   TEXT,
    UNIQUE (job_id, chunk_index)
);

CREATE INDEX idx_import_chunk_job ON import_chunk(job_id, chunk_index);

CREATE TABLE IF NOT EXISTS knowledge_integrity (
    timeline_id     VARCHAR(128) NOT NULL,
    kp_id           VARCHAR(128) NOT NULL,
    original_segments JSONB NOT NULL,
    revised_segments  JSONB NOT NULL,
    intact          BOOLEAN NOT NULL DEFAULT true,
    details         TEXT,
    checked_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (timeline_id, kp_id)
);

CREATE TABLE IF NOT EXISTS analysis_checkpoint (
    timeline_id     VARCHAR(128) PRIMARY KEY,
    last_event_id   UUID NOT NULL,
    analysis_version BIGINT NOT NULL,
    window_state    JSONB,
    computed_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
