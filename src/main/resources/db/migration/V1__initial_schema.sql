CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE storyboards (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    external_id VARCHAR(255) NOT NULL UNIQUE,
    title VARCHAR(512) NOT NULL,
    current_version BIGINT NOT NULL DEFAULT 0,
    knowledge_point_count INT NOT NULL DEFAULT 0,
    original_knowledge_points JSONB,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_storyboards_external_id ON storyboards(external_id);

CREATE TABLE storyboard_segments (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    storyboard_id UUID NOT NULL REFERENCES storyboards(id) ON DELETE CASCADE,
    segment_order INT NOT NULL,
    start_time_ms BIGINT NOT NULL,
    end_time_ms BIGINT NOT NULL,
    stimulus_intensity SMALLINT NOT NULL CHECK (stimulus_intensity BETWEEN 1 AND 5),
    has_reversal BOOLEAN NOT NULL DEFAULT FALSE,
    is_knowledge_point BOOLEAN NOT NULL DEFAULT FALSE,
    knowledge_point_id VARCHAR(255),
    has_scroll_inducement BOOLEAN NOT NULL DEFAULT FALSE,
    content_type VARCHAR(64) NOT NULL DEFAULT 'content',
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_time_order CHECK (end_time_ms > start_time_ms),
    CONSTRAINT chk_knowledge_point CHECK (
        (is_knowledge_point = FALSE AND knowledge_point_id IS NULL) OR
        (is_knowledge_point = TRUE AND knowledge_point_id IS NOT NULL)
    )
);

CREATE INDEX idx_segments_storyboard ON storyboard_segments(storyboard_id, segment_order);
CREATE INDEX idx_segments_storyboard_time ON storyboard_segments(storyboard_id, start_time_ms);
CREATE UNIQUE INDEX idx_segments_storyboard_order ON storyboard_segments(storyboard_id, segment_order);

CREATE TABLE event_log (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE DEFAULT uuid_generate_v4(),
    event_type VARCHAR(64) NOT NULL,
    storyboard_id UUID REFERENCES storyboards(id) ON DELETE CASCADE,
    aggregate_version BIGINT NOT NULL,
    payload JSONB NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    occurred_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX idx_event_log_storyboard_version ON event_log(storyboard_id, aggregate_version);
CREATE INDEX idx_event_log_unprocessed ON event_log(processed_at) WHERE processed_at IS NULL;

CREATE TABLE outbox_events (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    event_type VARCHAR(64) NOT NULL,
    topic VARCHAR(255) NOT NULL,
    key VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    headers JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    published_at TIMESTAMP WITH TIME ZONE,
    attempts INT NOT NULL DEFAULT 0,
    last_error TEXT
);

CREATE INDEX idx_outbox_unpublished ON outbox_events(published_at) WHERE published_at IS NULL;

CREATE TABLE risk_projections (
    id BIGSERIAL PRIMARY KEY,
    storyboard_id UUID NOT NULL REFERENCES storyboards(id) ON DELETE CASCADE,
    rule_version VARCHAR(32) NOT NULL,
    projection_version BIGINT NOT NULL DEFAULT 0,
    last_event_id BIGINT,
    computed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    total_risk_score DOUBLE PRECISION NOT NULL DEFAULT 0,
    results_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    UNIQUE(storyboard_id)
);

CREATE TABLE risk_findings (
    id BIGSERIAL PRIMARY KEY,
    projection_id BIGINT NOT NULL REFERENCES risk_projections(id) ON DELETE CASCADE,
    storyboard_id UUID NOT NULL REFERENCES storyboards(id) ON DELETE CASCADE,
    rule_id VARCHAR(64) NOT NULL,
    severity VARCHAR(32) NOT NULL CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH')),
    window_start_ms BIGINT,
    window_end_ms BIGINT,
    affected_segment_ids UUID[] NOT NULL DEFAULT '{}',
    evidence_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    suggestion TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_findings_storyboard ON risk_findings(storyboard_id);
CREATE INDEX idx_findings_projection ON risk_findings(projection_id);
CREATE INDEX idx_findings_rule ON risk_findings(storyboard_id, rule_id);

CREATE TABLE consumer_offsets (
    consumer_group VARCHAR(255) NOT NULL,
    topic_partition VARCHAR(255) NOT NULL,
    offset_val BIGINT NOT NULL DEFAULT 0,
    last_event_id BIGINT,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    PRIMARY KEY (consumer_group, topic_partition)
);

CREATE TABLE dead_letters (
    id BIGSERIAL PRIMARY KEY,
    original_topic VARCHAR(255) NOT NULL,
    original_partition INT NOT NULL,
    original_offset BIGINT NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    storyboard_id UUID,
    payload JSONB NOT NULL,
    error_message TEXT NOT NULL,
    error_stack_trace TEXT,
    retry_count INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 3,
    next_retry_at TIMESTAMP WITH TIME ZONE,
    resolved BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_dead_letters_unresolved ON dead_letters(resolved) WHERE resolved = FALSE;
CREATE INDEX idx_dead_letters_retry ON dead_letters(next_retry_at) WHERE resolved = FALSE AND next_retry_at IS NOT NULL;

CREATE TABLE import_jobs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    storyboard_id UUID NOT NULL REFERENCES storyboards(id) ON DELETE CASCADE,
    status VARCHAR(32) NOT NULL CHECK (status IN ('PENDING', 'RUNNING', 'PAUSED', 'COMPLETED', 'CANCELLED', 'FAILED')),
    total_segments INT NOT NULL DEFAULT 0,
    processed_segments INT NOT NULL DEFAULT 0,
    failed_segments INT NOT NULL DEFAULT 0,
    checkpoint INT NOT NULL DEFAULT 0,
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    cancelled_at TIMESTAMP WITH TIME ZONE,
    error_message TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_import_jobs_status ON import_jobs(status);
CREATE INDEX idx_import_jobs_storyboard ON import_jobs(storyboard_id);

CREATE TABLE import_batches (
    id BIGSERIAL PRIMARY KEY,
    import_job_id UUID NOT NULL REFERENCES import_jobs(id) ON DELETE CASCADE,
    batch_number INT NOT NULL,
    segment_data JSONB NOT NULL,
    processed BOOLEAN NOT NULL DEFAULT FALSE,
    processed_at TIMESTAMP WITH TIME ZONE,
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    UNIQUE(import_job_id, batch_number)
);

CREATE INDEX idx_import_batches_job ON import_batches(import_job_id, batch_number);
CREATE INDEX idx_import_batches_unprocessed ON import_batches(import_job_id) WHERE processed = FALSE;

CREATE TABLE projection_drift_checkpoints (
    id BIGSERIAL PRIMARY KEY,
    storyboard_id UUID NOT NULL REFERENCES storyboards(id) ON DELETE CASCADE,
    check_type VARCHAR(64) NOT NULL,
    incremental_result JSONB,
    full_result JSONB,
    drift_detected BOOLEAN NOT NULL DEFAULT FALSE,
    drift_details TEXT,
    checked_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_drift_storyboard ON projection_drift_checkpoints(storyboard_id, checked_at DESC);
