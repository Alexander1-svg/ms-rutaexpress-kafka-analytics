package com.ms_rutaexpress.kafka_analytics.controller;

import com.ms_rutaexpress.kafka_analytics.dto.AnalyticsSummaryResponse;
import com.ms_rutaexpress.kafka_analytics.dto.RecentShipmentEventResponse;
import com.ms_rutaexpress.kafka_analytics.dto.StatusAnalyticsResponse;
import com.ms_rutaexpress.kafka_analytics.service.AnalyticsQueryService;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Validated
@RestController
@RequestMapping("/api/v1/analytics")
public class AnalyticsController {

    private final AnalyticsQueryService queryService;

    public AnalyticsController(AnalyticsQueryService queryService) {
        this.queryService = queryService;
    }

    @GetMapping("/summary")
    public AnalyticsSummaryResponse summary() {
        return queryService.summary();
    }

    @GetMapping("/statuses")
    public StatusAnalyticsResponse statuses() {
        return queryService.statuses();
    }

    @GetMapping("/recent")
    public List<RecentShipmentEventResponse> recent(
            @RequestParam(defaultValue = "20") @Min(1) @Max(100) int limit
    ) {
        return queryService.recent(limit);
    }
}

