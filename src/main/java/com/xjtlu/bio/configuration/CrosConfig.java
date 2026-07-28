package com.xjtlu.bio.configuration;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;


@Configuration
public class CrosConfig implements WebMvcConfigurer{

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/**") // 1. 拦截所有的后端接口
                // 2. 允许的前端域名（Spring Boot 2.4+ 及 3.x 强烈建议用 allowedOriginPatterns 代替 allowedOrigins）
                .allowedOriginPatterns("*") 
                // .allowedOriginPatterns("http://localhost:5173", "http://wsl.dev:*") // 生产环境建议指定具体的前端域名
                .allowedMethods("GET", "POST", "PUT", "DELETE", "OPTIONS") // 3. 允许的 HTTP 请求方法
                .allowedHeaders("*") // 4. 允许携带的请求头（如 Authorization、Accept 等）
                .allowCredentials(true) // 5. 是否允许前端携带 Cookie 或凭证（Token）
                .maxAge(3600); // 6. 预检请求（OPTIONS）的缓存时间（单位：秒），避免频繁发送预检请求
    }

}
