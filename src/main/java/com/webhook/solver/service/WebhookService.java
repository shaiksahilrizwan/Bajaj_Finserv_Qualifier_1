package com.webhook.solver.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

@Service
public class WebhookService implements ApplicationRunner {
    
    private static final Logger logger = LoggerFactory.getLogger(WebhookService.class);
    private final RestTemplate restTemplate = new RestTemplate();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void run(ApplicationArguments args) throws Exception {
        try {
            // Step 1: Generate webhook
            String webhookUrl = generateWebhook();
            
            // Step 2: Get access token and webhook URL from response
            WebhookResponse response = parseWebhookResponse(webhookUrl);
            
            // Step 3: Solve SQL problem
            String sqlQuery = solveSqlProblem();
            
            // Step 4: Submit solution
            submitSolution(response.getWebhookUrl(), response.getAccessToken(), sqlQuery);
            
        } catch (Exception e) {
            logger.error("Error in webhook process: ", e);
        }
    }

    private String generateWebhook() throws Exception {
        String url = "https://bfhldevapigw.healthrx.co.in/hiring/generateWebhook/JAVA";
        
        Map<String, String> requestBody = new HashMap<>();
        requestBody.put("name", "John Doe");
        requestBody.put("regNo", "REG12347");
        requestBody.put("email", "john@example.com");
        
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        
        HttpEntity<Map<String, String>> request = new HttpEntity<>(requestBody, headers);
        
        logger.info("Sending POST request to generate webhook...");
        ResponseEntity<String> response = restTemplate.postForEntity(url, request, String.class);
        
        logger.info("Webhook generation response: {}", response.getBody());
        return response.getBody();
    }

    private WebhookResponse parseWebhookResponse(String responseBody) throws Exception {
        JsonNode jsonNode = objectMapper.readTree(responseBody);
        
        String webhookUrl = jsonNode.get("webhook").asText();
        String accessToken = jsonNode.get("accessToken").asText();
        
        logger.info("Parsed webhook URL: {}", webhookUrl);
        logger.info("Parsed access token: {}", accessToken);
        
        return new WebhookResponse(webhookUrl, accessToken);
    }

    private String solveSqlProblem() {
        // SQL Query to find highest salary not on 1st day of month
        String sqlQuery = """
            SELECT 
                p.AMOUNT as SALARY,
                CONCAT(e.FIRST_NAME, ' ', e.LAST_NAME) as NAME,
                TIMESTAMPDIFF(YEAR, e.DOB, CURDATE()) as AGE,
                d.DEPARTMENT_NAME
            FROM PAYMENTS p
            JOIN EMPLOYEE e ON p.EMP_ID = e.EMP_ID
            JOIN DEPARTMENT d ON e.DEPARTMENT = d.DEPARTMENT_ID
            WHERE DAY(p.PAYMENT_TIME) != 1
            AND p.AMOUNT = (
                SELECT MAX(AMOUNT) 
                FROM PAYMENTS 
                WHERE DAY(PAYMENT_TIME) != 1
            )
            """;
        
        logger.info("Generated SQL Query: {}", sqlQuery);
        return sqlQuery;
    }

    private void submitSolution(String webhookUrl, String accessToken, String sqlQuery) throws Exception {
        Map<String, String> requestBody = new HashMap<>();
        requestBody.put("finalQuery", sqlQuery);
        
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("Authorization", accessToken);
        
        HttpEntity<Map<String, String>> request = new HttpEntity<>(requestBody, headers);
        
        logger.info("Submitting solution to webhook: {}", webhookUrl);
        ResponseEntity<String> response = restTemplate.postForEntity(webhookUrl, request, String.class);
        
        logger.info("Solution submission response: {}", response.getBody());
        logger.info("Solution submitted successfully!");
    }

    private static class WebhookResponse {
        private final String webhookUrl;
        private final String accessToken;

        public WebhookResponse(String webhookUrl, String accessToken) {
            this.webhookUrl = webhookUrl;
            this.accessToken = accessToken;
        }

        public String getWebhookUrl() {
            return webhookUrl;
        }

        public String getAccessToken() {
            return accessToken;
        }
    }
}