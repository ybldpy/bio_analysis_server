package com.xjtlu.bio.configuration;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import com.xjtlu.bio.common.Result;

@RestControllerAdvice
public class GlobalExceptionHandler {




    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<String> handleNotValidParameters(MethodArgumentNotValidException e){
        return ResponseEntity.badRequest().body(e.getMessage());
    }
    
}
