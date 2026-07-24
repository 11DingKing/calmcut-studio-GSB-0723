CREATE TABLE consumer_processed_offsets (
    consumer_group VARCHAR(255) NOT NULL,
    topic VARCHAR(255) NOT NULL,
    partition INT NOT NULL,
    offset_val BIGINT NOT NULL,
    event_id UUID,
    event_type VARCHAR(64),
    storyboard_id UUID,
    processed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    PRIMARY KEY (consumer_group, topic, partition, offset_val)
);

CREATE INDEX idx_processed_storyboard ON consumer_processed_offsets(storyboard_id, consumer_group);

CREATE TABLE consumer_storyboard_versions (
    consumer_group VARCHAR(255) NOT NULL,
    storyboard_id UUID NOT NULL,
    last_processed_version BIGINT NOT NULL DEFAULT 0,
    processed_event_ids UUID[] NOT NULL DEFAULT '{}',
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    PRIMARY KEY (consumer_group, storyboard_id)
);

CREATE INDEX idx_storyboard_versions_group ON consumer_storyboard_versions(consumer_group);

ALTER TABLE storyboard_segments DROP CONSTRAINT IF EXISTS chk_time_order;
ALTER TABLE storyboard_segments ADD CONSTRAINT chk_time_order CHECK (end_time_ms > start_time_ms);
