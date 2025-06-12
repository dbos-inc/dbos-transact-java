package dev.dbos.transact.interceptor;

import dev.dbos.transact.workflow.Workflow;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class TransactInvocationHandlerTest {

    @Test
    void createProxy() {
    }

    @Test
    void invokeWorkflow() throws Throwable {

        OrderServiceImpl impl = new OrderServiceImpl();

        TransactInvocationHandler realHandler =
                new TransactInvocationHandler(impl);

        TransactInvocationHandler spyHandler = Mockito.spy(realHandler);

        OrderService proxy = (OrderService) Proxy.newProxyInstance(
                OrderService.class.getClassLoader(),
                new Class[]{OrderService.class},
                spyHandler
        );

        proxy.processOrder("test-item");

        // Assert
        Method expectedMethod = OrderService.class.getMethod("processOrder", String.class);
        verify(spyHandler, times(1)).handleWorkflow(eq(expectedMethod), any(),any());

    }
}

