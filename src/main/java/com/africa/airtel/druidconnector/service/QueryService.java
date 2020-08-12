package com.africa.airtel.druidconnector.service;

import com.africa.airtel.druidconnector.domain.SummarizingFunction;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Service;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
public class QueryService {

    @Autowired
    private DruidSqlService druidSqlService;

    private static final String SELECT_QUERY_GROUPING = "select :dimensions from :dataSource where :filters group by :groupBy";

    private static final ParameterizedTypeReference<List<Map<String, Object>>> LIST_OF_MAP_TYPE = new ParameterizedTypeReference<List<Map<String, Object>>>() {
    };

    public List<Map<String, Object>> query(String dataSource, SummarizingFunction summarizingFunction,
                                           List<String> dimensions, LinkedHashSet<String> groupBy, Map<String, Object> filters) {

        String sqlQuery = buildSqlQuery(dataSource, summarizingFunction, dimensions, groupBy, filters);
        log.info("Sql Query: {}", sqlQuery);

        return druidSqlService.execute(sqlQuery, LIST_OF_MAP_TYPE);
    }

    private String buildSqlQuery(String dataSource, SummarizingFunction summarizingFunction,
                                 List<String> dimensions, LinkedHashSet<String> groupBy, Map<String, Object> filters) {

        String dimensionsToSelect = getDimensionsToSelect(summarizingFunction, dimensions, groupBy);

        String filtersForSql = getFiltersAsString(filters);

        return SELECT_QUERY_GROUPING.replace(":dimensions", dimensionsToSelect)
                .replace(":dataSource", dataSource)
                .replace(":filters", filtersForSql)
                .replace(":groupBy", String.join(", ", groupBy));
    }

    // Currently just used start and end date
    // other filters can be applied (need to find a way whether it's equal or > or < ...)
    private String getFiltersAsString(Map<String, Object> filters) {
        return  "__time >= " + filters.get("startDate").toString() +
                " and __time < " + filters.get("endDate").toString();
    }

    private String getDimensionsToSelect(SummarizingFunction summarizingFunction, List<String> dimensions, LinkedHashSet<String> groupBy) {
        StringBuilder sb = new StringBuilder(String.join(", ", groupBy));

        if (!dimensions.isEmpty()) {
            sb.append(", ");

            List<String> dimensionsWithSummarizingFunction = dimensions.stream()
                    .map(dimension -> summarizingFunction.toString() + "(" + dimension + ")").collect(Collectors.toList());

            sb.append(String.join(", ", dimensionsWithSummarizingFunction));
        }
        return sb.toString();
    }
}
