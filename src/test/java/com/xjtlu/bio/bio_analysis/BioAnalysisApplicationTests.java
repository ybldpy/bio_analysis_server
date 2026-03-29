package com.xjtlu.bio.bio_analysis;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;


import jakarta.annotation.Resource;

@SpringBootTest
class BioAnalysisApplicationTests {



	@Test
	void contextLoads() {
	}

    @Test
    void testDuplicateInsert(){
        System.out.println("Testing duplicate insert:");
        //sampleService.testInsertDuplicate("1", 1);
    }

}
