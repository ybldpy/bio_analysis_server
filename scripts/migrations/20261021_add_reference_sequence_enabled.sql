-- One-time, additive migration for MySQL 8.0.12+.
-- Preflight: bio.bio_reference_sequence exists; enabled and idx_tax_id_enabled do not.
-- Existing references remain enabled. Import code must explicitly control when
-- newly uploaded references become available; this is not an upload-status field.
-- Keep existing indexes/constraints and all reference IDs and file paths unchanged.
-- Run each statement separately; MySQL DDL commits implicitly.

ALTER TABLE bio.bio_reference_sequence
    ADD COLUMN enabled BOOLEAN NOT NULL DEFAULT TRUE
        COMMENT 'Whether this reference may be used for new analysis candidate lookups',
    ALGORITHM=INSTANT;

ALTER TABLE bio.bio_reference_sequence
    ADD INDEX idx_tax_id_enabled (tax_id, enabled),
    ALGORITHM=INPLACE, LOCK=NONE;
