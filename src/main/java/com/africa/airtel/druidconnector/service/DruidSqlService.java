package com.africa.airtel.druidconnector.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.Collections;
import java.util.Map;

@Service
public class DruidSqlService {

    @Value("${druid.sql.url}")
    private String url;

    private RestTemplate restTemplate;

    @Autowired
    public DruidSqlService(RestTemplateBuilder restTemplateBuilder) {
        restTemplate = restTemplateBuilder.build();
    }

    public <T> T execute(String sqlQuery, ParameterizedTypeReference<T> parameterizedTypeReference) {
        return restTemplate.exchange(url, HttpMethod.POST, getEntity(sqlQuery), parameterizedTypeReference).getBody();
    }

    private HttpEntity<Map> getEntity(String sqlQuery) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        return new HttpEntity<>(Collections.singletonMap("query", sqlQuery), headers);
    }
}
