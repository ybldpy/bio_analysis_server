package com.xjtlu.bio.bio_analysis;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import com.xjtlu.bio.service.SampleService;

import jakarta.annotation.Resource;

@SpringBootTest
class BioAnalysisApplicationTests {

    @Resource
    private SampleService sampleService;

	@Test
	void contextLoads() {
	}

    @Test
    void testDuplicateInsert(){
        System.out.println("Testing duplicate insert:");
        //sampleService.testInsertDuplicate("1", 1);
    }

}
