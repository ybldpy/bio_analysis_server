package com.xjtlu.bio.configuration;

import java.util.List;
import java.util.stream.Collectors;

import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import com.xjtlu.bio.common.Result;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<String> handleNotValidParameters(MethodArgumentNotValidException e) {
        String errorMessage = e.getBindingResult()
                .getFieldErrors()
                .stream()
                .map(FieldError::getDefaultMessage) // 获取你在注解里写的 message
                .collect(Collectors.joining("; ")); // 用分号连接： "sampleName不能为空; isPair不能为空"

        return ResponseEntity.badRequest().body(errorMessage);
    }

}
