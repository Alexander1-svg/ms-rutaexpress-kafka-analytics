package com.ms_rutaexpress.kafka_analytics.controller;

import com.ms_rutaexpress.kafka_analytics.config.ApplicationConfiguration;
import com.ms_rutaexpress.kafka_analytics.dto.AnalyticsSummaryResponse;
import com.ms_rutaexpress.kafka_analytics.dto.RecentShipmentEventResponse;
import com.ms_rutaexpress.kafka_analytics.dto.StatusAnalyticsResponse;
import com.ms_rutaexpress.kafka_analytics.service.AnalyticsQueryService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(AnalyticsController.class)
@Import(ApplicationConfiguration.class)
class AnalyticsControllerTest {

    @MockitoBean
    private AnalyticsQueryService queryService;

    @Autowired
    private MockMvc mockMvc;

    @Test
    void returnsSummary() throws Exception {
        when(queryService.summary()).thenReturn(new AnalyticsSummaryResponse(
                3, 1, Map.of("DELIVERED", 2L), List.of()
        ));

        mockMvc.perform(get("/api/v1/analytics/summary"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalProcessed").value(3))
                .andExpect(jsonPath("$.duplicateEvents").value(1))
                .andExpect(jsonPath("$.byStatus.DELIVERED").value(2));
    }

    @Test
    void returnsStatusesAndRecentEvents() throws Exception {
        when(queryService.statuses()).thenReturn(new StatusAnalyticsResponse(Map.of("IN_TRANSIT", 4L)));
        when(queryService.recent(5)).thenReturn(List.of(new RecentShipmentEventResponse(
                "evt-1", "ENV-10", "IN_TRANSIT",
                Instant.parse("2026-11-15T20:30:00Z"), Instant.parse("2026-11-15T20:31:00Z")
        )));

        mockMvc.perform(get("/api/v1/analytics/statuses"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.byStatus.IN_TRANSIT").value(4));
        mockMvc.perform(get("/api/v1/analytics/recent").param("limit", "5"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].eventId").value("evt-1"));
    }

    @Test
    void rejectsRecentLimitOutsideAllowedRange() throws Exception {
        mockMvc.perform(get("/api/v1/analytics/recent").param("limit", "0"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("Solicitud inválida"));
    }
}
