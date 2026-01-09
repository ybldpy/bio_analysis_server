package com.xjtlu.bio;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@MapperScan("com.xjtlu.bio.mapper")
@ConfigurationPropertiesScan
public class BioAnalysisApplication {

	public static void main(String[] args) {
		SpringApplication.run(BioAnalysisApplication.class, args);
	}
	
}
