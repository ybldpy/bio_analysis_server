package com.xjtlu.bio.auth;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.web.SecurityFilterChain;

@Configuration
public class SecurityConfig {


    @Value("${api.login}")
    private String loginApi;


    @Value("${api.logout}")
    private String logout;

    @Bean
    SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        // http
        //         // 前后端分离常用 CSRF token；若先图省事，可暂时关闭
        //         .csrf(csrf -> csrf.disable())

        //         // 跨域（如前端 http://localhost:5173）
        //         .cors(cors -> {
        //         }) // 使用自定义 CorsConfigurationSource（见下）

        //         .authorizeHttpRequests(auth -> auth
        //                 .anyRequest().authenticated())
        //         // 使用表单登录，但只作为“处理登录请求”的过滤器，不用默认页面
        //         .formLogin(form -> form
        //                 .loginProcessingUrl(loginApi) // 前端POST用户名/密码
        //                 .successHandler((req, res, auth) -> res.setStatus(200))
        //                 .failureHandler((req, res, ex) -> res.sendError(401, "Bad credentials")))

        //         // 退出登录
        //         .logout(logout -> logout
        //                 .logoutUrl(this.logout)
        //                 .logoutSuccessHandler((req, res, auth) -> res.setStatus(200)))
        //         // Session 策略
        //         .sessionManagement(sm -> sm
        //                 .sessionFixation(sessionFixation -> sessionFixation.migrateSession()));

        // return http.build();


        http
        // 1. 关闭 CSRF
        .csrf(csrf -> csrf.disable())

        // 2. 配置跨域
        .cors(cors -> { })

        // 3. 修改这里：允许所有请求访问所有接口
        .authorizeHttpRequests(auth -> auth
            .anyRequest().permitAll() 
        )

        // 4. 表单登录（即使放行了，这些配置也可以留着，只是现在不会强制拦截你）
        .formLogin(form -> form
            .loginProcessingUrl(loginApi)
            .successHandler((req, res, auth) -> res.setStatus(200))
            .failureHandler((req, res, ex) -> res.sendError(401, "Bad credentials")))

        // 5. 退出登录
        .logout(logout -> logout
            .logoutUrl(this.logout)
            .logoutSuccessHandler((req, res, auth) -> res.setStatus(200)))

        // 6. Session 策略
        .sessionManagement(sm -> sm
            .sessionFixation(sf -> sf.migrateSession()));

    return http.build();
    }


    
}