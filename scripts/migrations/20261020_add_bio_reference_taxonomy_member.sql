-- Run this migration explicitly during deployment, not on each application start.
-- Maps a requested NCBI taxId to explicit reference taxIds without rewriting the
-- original taxonomy identifiers in bio_reference_sequence.
CREATE TABLE IF NOT EXISTS bio_reference_taxonomy_member (
    id BIGINT NOT NULL AUTO_INCREMENT,
    tax_id INT NOT NULL COMMENT 'Requested taxonomy ID',
    member_tax_id INT NOT NULL COMMENT 'Original taxonomy ID in bio_reference_sequence',
    sort_order INT NOT NULL DEFAULT 0 COMMENT 'Stable member order only; not a score or priority',
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    UNIQUE KEY uk_reference_taxonomy_member (tax_id, member_tax_id),
    CONSTRAINT chk_ref_taxonomy_target_positive CHECK (tax_id > 0),
    CONSTRAINT chk_ref_taxonomy_member_positive CHECK (member_tax_id > 0),
    CONSTRAINT chk_ref_taxonomy_order_nonnegative CHECK (sort_order >= 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Seed the 7 non-identity mappings (18 rows) from the 2026-10-20 frontend
-- dictionary. Identity mappings need no row: the service keeps exact matching
-- when a target has no configured members.
--
-- Preserve each existing target's complete configuration on a repeated run:
-- do not change order, append removed members, or overwrite manual edits.
-- A target is seeded only if it has no mapping rows at all. Deliberately deleting
-- every row for a seeded target and then rerunning this migration restores that
-- target's defaults. Rerunning migrations is not the configuration update path.
INSERT INTO bio_reference_taxonomy_member (tax_id, member_tax_id, sort_order)
SELECT seed.tax_id, seed.member_tax_id, seed.sort_order
FROM (
    SELECT 11320 AS tax_id, 641809 AS member_tax_id, 0 AS sort_order
    UNION ALL SELECT 11320, 93838, 1
    UNION ALL SELECT 11320, 130760, 2
    UNION ALL SELECT 11320, 488241, 3
    UNION ALL SELECT 11320, 335341, 4
    UNION ALL SELECT 11320, 211044, 5
    UNION ALL SELECT 11320, 1332244, 6
    UNION ALL SELECT 11520, 518987, 0
    UNION ALL SELECT 11552, 11553, 0
    UNION ALL SELECT 2560526, 11224, 0
    UNION ALL SELECT 11250, 11250, 0
    UNION ALL SELECT 11250, 208893, 1
    UNION ALL SELECT 12637, 11053, 0
    UNION ALL SELECT 12637, 11060, 1
    UNION ALL SELECT 12637, 11069, 2
    UNION ALL SELECT 12637, 11070, 3
    UNION ALL SELECT 1003835, 1003835, 0
    UNION ALL SELECT 1003835, 992212, 1
) AS seed
LEFT JOIN bio_reference_taxonomy_member AS existing
    ON existing.tax_id = seed.tax_id
WHERE existing.id IS NULL;
