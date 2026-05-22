package com.example.orderapi.exception;

import com.example.orderapi.dto.ErrorResponse;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.stream.Collectors;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidation(MethodArgumentNotValidException ex) {
        String message = ex.getBindingResult().getFieldErrors().stream()
                .map(error -> error.getField() + ": " + error.getDefaultMessage())
                .collect(Collectors.joining(", "));

        ErrorResponse errorResponse = new ErrorResponse(
                "ValidationError",
                message,
                null
        );

        return ResponseEntity.badRequest().body(errorResponse);
    }

    @ExceptionHandler(PublishException.class)
    public ResponseEntity<ErrorResponse> handlePublishError(PublishException ex) {
        ErrorResponse errorResponse = new ErrorResponse(
                "ServiceUnavailable",
                ex.getMessage(),
                ex.getCorrelationId()
        );

        HttpHeaders headers = new HttpHeaders();
        headers.add("Retry-After", "5");

        return ResponseEntity.status(503)
                .headers(headers)
                .body(errorResponse);
    }
}
