-- Pipeline events are append-only. content contains plain text, not JSON.
CREATE TABLE IF NOT EXISTS bio_pipeline_log (
    id BIGINT NOT NULL AUTO_INCREMENT,
    pipeline_id BIGINT NOT NULL,
    stage_id BIGINT NULL,
    stage_version INT NULL COMMENT 'Version of the execution that produced this event',
    level VARCHAR(16) NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    content LONGTEXT NOT NULL,
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    KEY idx_pipeline_log_pipeline_time (pipeline_id, created_at, id),
    KEY idx_pipeline_log_stage_time (stage_id, created_at, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
