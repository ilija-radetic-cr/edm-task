package com.example.orderapi.controller;

import com.example.orderapi.dto.CreateOrderRequest;
import com.example.orderapi.dto.OrderResponse;
import com.example.orderapi.exception.PublishException;
import com.example.orderapi.exception.GlobalExceptionHandler;
import com.example.orderapi.service.OrderService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class OrderControllerTest {

    private MockMvc mockMvc;
    private TestOrderService orderService;

    @BeforeEach
    void setUp() {
        orderService = new TestOrderService();

        LocalValidatorFactoryBean validator = new LocalValidatorFactoryBean();
        validator.afterPropertiesSet();

        mockMvc = MockMvcBuilders
                .standaloneSetup(new OrderController(orderService))
                .setControllerAdvice(new GlobalExceptionHandler())
                .setValidator(validator)
                .build();
    }

    @Test
    void createOrderPublishesCommandAndReturnsAccepted() throws Exception {
        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("X-Correlation-Id", "corr-1")
                        .content("""
                                {
                                  "orderId": "ORD-1",
                                  "itemId": "item-1",
                                  "quantity": 2
                                }
                                """))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.orderId").value("ORD-1"))
                .andExpect(jsonPath("$.correlationId").value("corr-1"))
                .andExpect(jsonPath("$.status").value("accepted"));
    }

    @Test
    void createOrderRejectsInvalidRequestBeforePublishing() throws Exception {
        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "orderId": "",
                                  "itemId": "item-1",
                                  "quantity": -1
                                }
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("ValidationError"));

        org.assertj.core.api.Assertions.assertThat(orderService.calls).isZero();
    }

    @Test
    void createOrderReturnsServiceUnavailableWhenKafkaPublishFails() throws Exception {
        orderService.failPublish = true;

        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("X-Correlation-Id", "corr-2")
                        .content("""
                                {
                                  "orderId": "ORD-2",
                                  "itemId": "item-1",
                                  "quantity": 2
                                }
                                """))
                .andExpect(status().isServiceUnavailable())
                .andExpect(header().string("Retry-After", "5"))
                .andExpect(jsonPath("$.error").value("ServiceUnavailable"))
                .andExpect(jsonPath("$.correlationId").value("corr-2"));
    }

    private static class TestOrderService extends OrderService {
        private int calls;
        private boolean failPublish;

        TestOrderService() {
            super(null, "orders", 5000);
        }

        @Override
        public OrderResponse publishOrderCreated(CreateOrderRequest request, String correlationId) {
            calls++;
            if (failPublish) {
                throw new PublishException("Failed to publish event", correlationId);
            }
            return new OrderResponse(request.getOrderId(), correlationId, "accepted");
        }
    }
}
