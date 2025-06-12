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

        String result = proxy.processOrder("test-item");
        assertEquals("Processed: test-item", result);

        // Assert
        Method expectedMethod = OrderService.class.getMethod("processOrder", String.class);
        verify(spyHandler, times(1)).handleWorkflow(eq(expectedMethod), any(),any());

    }

    @Test
    void invokeStep() throws Throwable {

        OrderServiceImpl impl = new OrderServiceImpl();

        TransactInvocationHandler realHandler =
                new TransactInvocationHandler(impl);

        TransactInvocationHandler spyHandler = Mockito.spy(realHandler);

        OrderService proxy = (OrderService) Proxy.newProxyInstance(
                OrderService.class.getClassLoader(),
                new Class[]{OrderService.class},
                spyHandler
        );

        String result = proxy.reserveInventory("123",21, 1);
        assertEquals("123211",result);

        // Assert
        Method expectedMethod = OrderService.class.getMethod("reserveInventory", String.class, int.class, int.class);
        verify(spyHandler, times(1)).handleStep(eq(expectedMethod), any(),any());

    }

    @Test
    void invokeTransaction() throws Throwable {

        OrderServiceImpl impl = new OrderServiceImpl();

        TransactInvocationHandler realHandler =
                new TransactInvocationHandler(impl);

        TransactInvocationHandler spyHandler = Mockito.spy(realHandler);

        OrderService proxy = (OrderService) Proxy.newProxyInstance(
                OrderService.class.getClassLoader(),
                new Class[]{OrderService.class},
                spyHandler
        );

        String result = proxy.chargeCustomer("123",45.23);
        assertEquals("12345.23",result);

        // Assert
        Method expectedMethod = OrderService.class.getMethod("chargeCustomer", String.class, double.class);
        verify(spyHandler, times(1)).handleTransaction(eq(expectedMethod), any(),any());

    }
}

