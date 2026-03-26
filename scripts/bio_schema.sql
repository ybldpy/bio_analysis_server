-- MySQL dump 10.13  Distrib 8.0.45, for Linux (x86_64)
--
-- Host: localhost    Database: bio
-- ------------------------------------------------------
-- Server version	8.0.45-0ubuntu0.22.04.1

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `bio_analysis_pipeline`
--

DROP TABLE IF EXISTS `bio_analysis_pipeline`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `bio_analysis_pipeline` (
  `pipeline_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Primary Key',
  `pipeline_type` int NOT NULL,
  `status` int NOT NULL DEFAULT '0',
  `analysis_pipeline_name` varchar(512) NOT NULL,
  `CREATE_TIME` datetime NOT NULL,
  `project_id` bigint NOT NULL,
  PRIMARY KEY (`pipeline_id`),
  UNIQUE KEY `uk_project_analysis_name` (`project_id`,`analysis_pipeline_name`),
  CONSTRAINT `bio_analysis_pipeline_ibfk_1` FOREIGN KEY (`project_id`) REFERENCES `bio_project` (`pid`)
) ENGINE=InnoDB AUTO_INCREMENT=40 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `bio_pipeline_input_file`
--

DROP TABLE IF EXISTS `bio_pipeline_input_file`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `bio_pipeline_input_file` (
  `input_file_id` bigint NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `pipeline_id` bigint NOT NULL COMMENT '所属pipeline run ID',
  `file_role` int DEFAULT NULL COMMENT '文件角色: R1/R2/assembly/reference/consensus等',
  `file_name` varchar(512) NOT NULL COMMENT '逻辑名称（用户看到的名字）',
  `original_filename` varchar(512) NOT NULL COMMENT '原始文件名',
  `file_path` varchar(1024) NOT NULL COMMENT '存储路径（本地/S3/OSS）',
  `status` int NOT NULL DEFAULT '0' COMMENT '状态: 0 not upload, 1 uploading, 2 uploaded',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (`input_file_id`),
  KEY `idx_pipeline_id` (`pipeline_id`),
  KEY `idx_file_role` (`file_role`),
  CONSTRAINT `bio_pipeline_input_file_ibfk_1` FOREIGN KEY (`pipeline_id`) REFERENCES `bio_analysis_pipeline` (`pipeline_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='pipeline输入文件表';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `bio_pipeline_stage`
--

DROP TABLE IF EXISTS `bio_pipeline_stage`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `bio_pipeline_stage` (
  `stage_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'Primary Key',
  `pipeline_id` bigint DEFAULT NULL,
  `stage_index` int NOT NULL,
  `stage_name` varchar(255) NOT NULL,
  `status` int NOT NULL,
  `start_time` datetime DEFAULT NULL,
  `end_time` datetime DEFAULT NULL,
  `input_url` varchar(500) DEFAULT NULL,
  `output_url` varchar(500) DEFAULT NULL,
  `stage_type` int NOT NULL,
  `parameters` longtext,
  `version` int NOT NULL DEFAULT '0',
  PRIMARY KEY (`stage_id`),
  KEY `pipeline_id` (`pipeline_id`)
) ENGINE=InnoDB AUTO_INCREMENT=79 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `bio_project`
--

DROP TABLE IF EXISTS `bio_project`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `bio_project` (
  `pid` bigint NOT NULL AUTO_INCREMENT,
  `project_name` varchar(255) NOT NULL,
  `description` varchar(255) NOT NULL,
  `created_by` bigint DEFAULT NULL,
  PRIMARY KEY (`pid`),
  UNIQUE KEY `project_name` (`project_name`),
  KEY `created_by` (`created_by`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `bio_refseq`
--

DROP TABLE IF EXISTS `bio_refseq`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `bio_refseq` (
  `ref_id` bigint NOT NULL AUTO_INCREMENT,
  `refseq_name` varchar(255) NOT NULL,
  `org_type` int NOT NULL,
  `source_db` varchar(255) NOT NULL,
  `refseq_path` varchar(500) NOT NULL,
  `tax_id` int NOT NULL,
  `meta` longtext,
  `accessions` varchar(1024) NOT NULL,
  `is_segment` tinyint(1) NOT NULL,
  `tax_rank` varchar(255) NOT NULL DEFAULT 'no rank',
  PRIMARY KEY (`ref_id`),
  KEY `idx_bio_refseq_tax_id` (`tax_id`)
) ENGINE=InnoDB AUTO_INCREMENT=118965 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `bio_refseq_meta`
--

DROP TABLE IF EXISTS `bio_refseq_meta`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `bio_refseq_meta` (
  `refseq_id` bigint NOT NULL AUTO_INCREMENT,
  `accession` varchar(64) NOT NULL,
  `source_db` varchar(64) NOT NULL,
  `tax_id` int DEFAULT NULL,
  `organism_name` varchar(255) DEFAULT NULL,
  `genome_length` int NOT NULL,
  `completeness` varchar(32) DEFAULT NULL,
  `is_annotated` tinyint(1) NOT NULL DEFAULT '0',
  `gene_count` int DEFAULT NULL,
  `protein_count` int DEFAULT NULL,
  `segment` varchar(64) DEFAULT NULL,
  `bioproject` varchar(64) DEFAULT NULL,
  `release_date` datetime DEFAULT NULL,
  `update_date` datetime DEFAULT NULL,
  `raw_metadata` longtext,
  `org_type` int NOT NULL,
  `path` varchar(500) DEFAULT NULL,
  PRIMARY KEY (`refseq_id`)
) ENGINE=InnoDB AUTO_INCREMENT=55741 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `bio_role`
--

DROP TABLE IF EXISTS `bio_role`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `bio_role` (
  `rid` int NOT NULL AUTO_INCREMENT COMMENT 'Primary Key',
  `role_name` varchar(255) NOT NULL,
  `description` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`rid`),
  UNIQUE KEY `role_name` (`role_name`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `bio_sample`
--

DROP TABLE IF EXISTS `bio_sample`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `bio_sample` (
  `sid` bigint NOT NULL AUTO_INCREMENT COMMENT 'Primary Key',
  `sample_name` varchar(255) NOT NULL,
  `is_pair` tinyint(1) NOT NULL,
  `read1_url` varchar(500) NOT NULL DEFAULT '',
  `read2_url` varchar(500) DEFAULT NULL,
  `project_id` bigint NOT NULL,
  `created_by` int DEFAULT NULL,
  `sample_type` int NOT NULL,
  `read1_upload_status` int NOT NULL DEFAULT '0',
  `read2_upload_status` int DEFAULT '0',
  PRIMARY KEY (`sid`),
  UNIQUE KEY `sample_name` (`sample_name`,`project_id`),
  KEY `created_by` (`created_by`),
  KEY `project_id` (`project_id`)
) ENGINE=InnoDB AUTO_INCREMENT=51 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `bio_tax_host`
--

DROP TABLE IF EXISTS `bio_tax_host`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `bio_tax_host` (
  `tax_host_id` int NOT NULL AUTO_INCREMENT COMMENT 'Primary Key',
  `tax_id` int NOT NULL,
  `host_name` varchar(255) NOT NULL,
  PRIMARY KEY (`tax_host_id`),
  UNIQUE KEY `uniq_tax_host` (`tax_id`,`host_name`),
  KEY `idx_bio_tax_host_tax_id` (`tax_id`),
  KEY `idx_bio_tax_host_host_name` (`host_name`)
) ENGINE=InnoDB AUTO_INCREMENT=375806 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `bio_user`
--

DROP TABLE IF EXISTS `bio_user`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `bio_user` (
  `uid` bigint NOT NULL AUTO_INCREMENT COMMENT 'Primary Key',
  `name` varchar(255) NOT NULL,
  `password` varchar(255) NOT NULL,
  `role_id` int DEFAULT NULL,
  PRIMARY KEY (`uid`),
  UNIQUE KEY `name` (`name`),
  KEY `role_id` (`role_id`),
  CONSTRAINT `bio_user_ibfk_1` FOREIGN KEY (`role_id`) REFERENCES `bio_role` (`rid`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2026-03-25 22:21:48
