-- Run this migration explicitly during deployment, not on each application start.
-- Stores frontend display targets only; reference membership remains separately
-- configured in bio_reference_taxonomy_member.
CREATE TABLE IF NOT EXISTS bio_pipeline_target (
    id BIGINT NOT NULL AUTO_INCREMENT,
    label VARCHAR(255) NOT NULL COMMENT 'Display label for the frontend target option',
    tax_id INT NOT NULL COMMENT 'Taxonomy ID submitted when creating a pipeline',
    pipeline_type INT NOT NULL COMMENT 'Pipeline type submitted with the target',
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    sort_order INT NOT NULL DEFAULT 0 COMMENT 'Stable frontend display order',
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    UNIQUE KEY uk_pipeline_target_type_tax_id (pipeline_type, tax_id),
    KEY idx_pipeline_target_enabled_order (enabled, sort_order, id),
    CONSTRAINT chk_pipeline_target_tax_id_positive CHECK (tax_id > 0),
    CONSTRAINT chk_pipeline_target_type_positive CHECK (pipeline_type > 0),
    CONSTRAINT chk_pipeline_target_enabled_boolean CHECK (enabled IN (0, 1)),
    CONSTRAINT chk_pipeline_target_order_nonnegative CHECK (sort_order >= 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Seed exactly the 67 options in docs/frontend/virus-taxonomy-options.json at
-- 2026-10-20, preserving their labels, taxonomy IDs, pipeline types and array order.
-- Insert missing (pipeline_type, tax_id) keys only: a repeated run must not
-- overwrite existing labels, enabled status or display order.
-- Deliberately deleting a seeded key and rerunning restores that default row.
-- Rerunning this migration is not the configuration maintenance path.
INSERT INTO bio_pipeline_target (label, tax_id, pipeline_type, enabled, sort_order)
SELECT seed.label, seed.tax_id, seed.pipeline_type, TRUE, seed.sort_order
FROM (
    SELECT '新冠病毒（SARS-CoV-2）' AS label, 2697049 AS tax_id, 101 AS pipeline_type, 0 AS sort_order
    UNION ALL SELECT '猴痘病毒', 10244, 100, 1
    UNION ALL SELECT '甲型流感病毒（A）', 11320, 100, 2
    UNION ALL SELECT '乙型流感病毒（B）', 11520, 100, 3
    UNION ALL SELECT '丙型流感病毒（C）', 11552, 100, 4
    UNION ALL SELECT '人副流感病毒1型', 12730, 100, 5
    UNION ALL SELECT '人副流感病毒2型', 2560525, 100, 6
    UNION ALL SELECT '人副流感病毒3型', 11216, 100, 7
    UNION ALL SELECT '人副流感病毒4型', 2560526, 100, 8
    UNION ALL SELECT '人呼吸道合胞病毒（RSV）', 11250, 100, 9
    UNION ALL SELECT '人腺病毒 1 型', 10533, 100, 10
    UNION ALL SELECT '人腺病毒 2 型', 10515, 100, 11
    UNION ALL SELECT '人腺病毒 5 型', 28285, 100, 12
    UNION ALL SELECT '人腺病毒 6 型', 10534, 100, 13
    UNION ALL SELECT '人腺病毒 7 型', 10519, 100, 14
    UNION ALL SELECT '人腺病毒 14 型', 10521, 100, 15
    UNION ALL SELECT '人腺病毒 35 型', 10522, 100, 16
    UNION ALL SELECT '人腺病毒 41 型', 10524, 100, 17
    UNION ALL SELECT '人腺病毒 54 型', 651580, 100, 18
    UNION ALL SELECT 'Human mastadenovirus A', 129875, 100, 19
    UNION ALL SELECT 'Human mastadenovirus B', 108098, 100, 20
    UNION ALL SELECT 'Human mastadenovirus C', 129951, 100, 21
    UNION ALL SELECT 'Human mastadenovirus D', 130310, 100, 22
    UNION ALL SELECT 'Human mastadenovirus E', 130308, 100, 23
    UNION ALL SELECT 'Human mastadenovirus F', 130309, 100, 24
    UNION ALL SELECT '人偏肺病毒（hMPV）', 162145, 100, 25
    UNION ALL SELECT '诺如病毒 GI（本 taxId 记录）', 122928, 100, 26
    UNION ALL SELECT '诺如病毒 GII（本 taxId 记录）', 122929, 100, 27
    UNION ALL SELECT '诺如病毒 GII.17', 552592, 100, 28
    UNION ALL SELECT '诺如病毒 GII.2', 490039, 100, 29
    UNION ALL SELECT 'Norovirus GI/Hu/JP/2007/GI.P3_GI.3/Shimizu/KK2866', 1529909, 100, 30
    UNION ALL SELECT 'Norovirus GII/Hu/JP/2007/GII.P15_GII.15/Sapporo/HK299', 1529918, 100, 31
    UNION ALL SELECT 'Norovirus GII/Hu/JP/2011/GII/Yuzawa/Gira2HS', 1529924, 100, 32
    UNION ALL SELECT '肠道病毒 A（本 taxId 记录）', 138948, 100, 33
    UNION ALL SELECT '肠道病毒 B（本 taxId 记录）', 138949, 100, 34
    UNION ALL SELECT '肠道病毒 C（本 taxId 记录）', 138950, 100, 35
    UNION ALL SELECT '肠道病毒 D（本 taxId 记录）', 138951, 100, 36
    UNION ALL SELECT '肠道病毒 A71', 39054, 100, 37
    UNION ALL SELECT '肠道病毒 D68', 42789, 100, 38
    UNION ALL SELECT '柯萨奇病毒 A2', 33757, 100, 39
    UNION ALL SELECT '柯萨奇病毒 A6', 86107, 100, 40
    UNION ALL SELECT '柯萨奇病毒 B3', 12072, 100, 41
    UNION ALL SELECT '柯萨奇病毒 B4', 12073, 100, 42
    UNION ALL SELECT '轮状病毒 A', 28875, 100, 43
    UNION ALL SELECT '人轮状病毒 B', 10942, 100, 44
    UNION ALL SELECT '轮状病毒 C', 36427, 100, 45
    UNION ALL SELECT 'Sapovirus Hu/Dresden/pJG-Sap01/DE', 291175, 100, 46
    UNION ALL SELECT 'Sapovirus Hu/GI/Sapporo/MT-2010/1982', 758863, 100, 47
    UNION ALL SELECT 'Sapovirus Hu/Nagoya/NGY-1/2012/JPN', 1280920, 100, 48
    UNION ALL SELECT '星状病毒 MLB1', 568715, 100, 49
    UNION ALL SELECT '星状病毒 VA1', 645687, 100, 50
    UNION ALL SELECT 'Human astrovirus BF34', 1518575, 100, 51
    UNION ALL SELECT 'Mamastrovirus 1', 1239565, 100, 52
    UNION ALL SELECT 'Human astrovirus（未分类条目，非全部人星状病毒）', 1868658, 100, 53
    UNION ALL SELECT '甲肝病毒', 12092, 100, 54
    UNION ALL SELECT '风疹病毒', 11041, 100, 55
    UNION ALL SELECT '基孔肯雅病毒', 37124, 100, 56
    UNION ALL SELECT '登革热病毒', 12637, 100, 57
    UNION ALL SELECT '大别班达病毒（SFTSV）', 1003835, 100, 58
    UNION ALL SELECT 'Orthohantavirus hantanense', 3052480, 100, 59
    UNION ALL SELECT 'Orthohantavirus seoulense', 3052498, 100, 60
    UNION ALL SELECT 'Orthohantavirus puumalaense', 3052493, 100, 61
    UNION ALL SELECT 'Orthohantavirus dobravaense', 3052477, 100, 62
    UNION ALL SELECT 'Orthohantavirus sinnombreense', 3052499, 100, 63
    UNION ALL SELECT 'Orthohantavirus andesense', 1980456, 100, 64
    UNION ALL SELECT 'HIV-1', 11676, 100, 65
    UNION ALL SELECT 'HIV-2', 11709, 100, 66
) AS seed
LEFT JOIN bio_pipeline_target AS existing
    ON existing.pipeline_type = seed.pipeline_type
    AND existing.tax_id = seed.tax_id
WHERE existing.id IS NULL;
