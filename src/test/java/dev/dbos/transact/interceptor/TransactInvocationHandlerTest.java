package dev.dbos.transact.interceptor;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.workflow.Workflow;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class TransactInvocationHandlerTest {

    /* @Test
    void createProxy() {

        DBOSConfig config = new DBOSConfig.Builder().name("orderservice").build();
        DBOS.initialize(config);
        DBOS dbos = DBOS.getInstance();

        OrderService proxy = dbos.<OrderService>Workflow()
                .interfaceClass(OrderService.class)
                .implementation(new OrderServiceImpl())
                .build();

        String result = proxy.processOrder("test-item");
        assertEquals("Processed: test-item", result);
    } */

    @Test
    void invokeWorkflow() throws Throwable {

        OrderServiceImpl impl = new OrderServiceImpl();

        DBOSExecutor executor = mock(DBOSExecutor.class) ;

        doNothing().when(executor).preInvokeWorkflow(anyString(), anyString(), anyString(), anyString(), any(Object[].class));

        TransactInvocationHandler realHandler =
                new TransactInvocationHandler(impl, executor);

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

        DBOSExecutor executor = mock(DBOSExecutor.class) ;
        doNothing().when(executor).preInvokeWorkflow(anyString(), anyString(), anyString(), anyString(), any(Object[].class));

        TransactInvocationHandler realHandler =
                new TransactInvocationHandler(impl, executor);

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

        DBOSExecutor executor = mock(DBOSExecutor.class) ;
        doNothing().when(executor).preInvokeWorkflow(anyString(), anyString(), anyString(), anyString(), any(Object[].class));


        TransactInvocationHandler realHandler =
                new TransactInvocationHandler(impl , executor);

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

