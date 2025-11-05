package com.xjtlu.bio.service;

import java.io.File;
import java.io.InputStream;
import java.time.Duration;
import java.util.Map;

public interface StorageService {

    /* ==================== 写入 ==================== */

    /**
     * 将数据写入到指定 key。实现方应在 finally 中关闭 inputStream。
     * @param key  逻辑路径/对象名，例如 "samples/2025/11/abc.fastq.gz"
     * @param data 流（实现方负责读完并关闭）
     * @param opts 写入选项（可为 null）
     * @return     写入结果（包含 etag/实际大小等，便于审计与幂等）
     * @throws StorageException 写入失败
     */
    PutResult putObject(String key, InputStream data, PutOptions opts);

    /** 便捷重载：最小化参数（默认覆盖、contentType=application/octet-stream）。 */
    default PutResult putObject(String key, InputStream data) {
        return putObject(key, data, PutOptions.builder().build());
    }

    /* ==================== 读取 ==================== */
    GetObjectResult getObject(String key, String writeToPath);

    /**
     * 获取只读流与元信息（若实现不方便可抛 UnsupportedOperationException）。
     * 调用方负责关闭返回的 InputStream。
     */
    ObjectStat getObjectStream(String key);

    /* ==================== 管理 ==================== */

    boolean exists(String key);

    /** 不存在也不报错，返回是否真的删掉了东西。 */
    boolean delete(String key);

    /**
     * 生成临时访问链接（本地存储可返回 null 或抛 UnsupportedOperationException）。
     */
    default String presignedGetUrl(String key, Duration ttl) throws StorageException {
        throw new UnsupportedOperationException("presigned url not supported");
    }

    /* ==================== DTOs ==================== */

    record PutResult(boolean success, Exception e) {}
    record GetObjectResult(boolean success, File objectFile,Exception e){}
    

    record ObjectStat(String key, long size, String contentType,
                      Map<String,String> metadata, InputStream stream) {}

    /* ==================== 选项 ==================== */

    final class PutOptions {
        public final boolean overwrite;       // true: 覆盖；false: 若存在则失败
        public final String contentType;      // e.g. "application/gzip"
        public final Long size;               // 若已知可传；未知可为 null
        public final String md5Base64;        // 可选完整性校验
        public final Map<String,String> metadata; // 自定义元数据（如 sampleId）

        private PutOptions(Builder b) {
            this.overwrite   = b.overwrite;
            this.contentType = b.contentType;
            this.size        = b.size;
            this.md5Base64   = b.md5Base64;
            this.metadata    = b.metadata;
        }
        public static Builder builder(){ return new Builder(); }
        public static final class Builder {
            private boolean overwrite = true;
            private String contentType = "application/octet-stream";
            private Long size;
            private String md5Base64;
            private Map<String,String> metadata = Map.of();

            public Builder overwrite(boolean v){ this.overwrite = v; return this; }
            public Builder contentType(String v){ this.contentType = v; return this; }
            public Builder size(Long v){ this.size = v; return this; }
            public Builder md5Base64(String v){ this.md5Base64 = v; return this; }
            public Builder metadata(Map<String,String> v){ this.metadata = v; return this; }
            public PutOptions build(){ return new PutOptions(this); }
        }
    }

   
}
