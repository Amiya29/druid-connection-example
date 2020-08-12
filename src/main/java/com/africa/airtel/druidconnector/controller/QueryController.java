package com.africa.airtel.druidconnector.controller;

import com.africa.airtel.druidconnector.domain.SummarizingFunction;
import com.africa.airtel.druidconnector.service.QueryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

@RestController
public class QueryController {

    @Autowired
    private QueryService queryService;

    @GetMapping("/query")
    public List<Map<String, Object>> query(@RequestParam String dataSource,
                                           @RequestParam SummarizingFunction summarizingFunction,
                                           @RequestParam List<String> dimensions,
                                           @RequestParam LinkedHashSet<String> groupBy,
                                           @RequestParam String startDate,
                                           @RequestParam String endDate) {
        Map<String, Object> filters = new HashMap<>();
        filters.put("startDate", startDate);
        filters.put("endDate", endDate);

        return queryService.query(dataSource, summarizingFunction, dimensions, groupBy, filters);
    }
}
