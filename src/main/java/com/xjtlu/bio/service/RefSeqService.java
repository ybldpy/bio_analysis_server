package com.xjtlu.bio.service;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class RefSeqService {


    @Value("${refSeq.virus}")
    private String virusPath;


    @Value("${refSeq.virus-index}")
    private String virusIndexPath;

    private Map<String, Object> virusIndex;

    private static final String ACCESSION_KEY = "accession";

    public RefSeqService(){
        
    }



    private void loadVirusIndex() throws FileNotFoundException{

        FileInputStream fileInputStream = new FileInputStream(this.virusIndexPath);
        Scanner sc = new Scanner(fileInputStream, "UTF-8");
        ObjectMapper mapper = new ObjectMapper();

        while (sc.hasNextLine()) {
            String row = sc.nextLine();
            if (row.isEmpty()) {
                continue;
            }
            try {
                Map m = mapper.readValue(row, Map.class);
                Object accessionObj = m.get(ACCESSION_KEY);
                if(accessionObj != null && accessionObj.getClass().equals(String.class)){
                    String accession = (String) accessionObj;
                    virusIndex.put(accession, m);
                }

            } catch (JsonProcessingException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            
        }

        
        
    }
    private void init(){
        this.loadVirusIndex();
    }



    
    private String queryRefSeqPath(String queryRefSeqAccession){
        Object o = virusIndex.get(queryRefSeqAccession);
        if(o == null){return null;}
        return String.format("%s/%s", virusPath, queryRefSeqAccession);    
    }





}
