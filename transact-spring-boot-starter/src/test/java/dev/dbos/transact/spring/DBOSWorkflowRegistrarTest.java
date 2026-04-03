package dev.dbos.transact.spring;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.workflow.Workflow;

import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;

class DBOSWorkflowRegistrarTest {

  static class BeanWithWorkflow {
    @Workflow
    public String myWorkflow() {
      return "result";
    }
  }

  static class BeanWithoutWorkflow {
    public String regularMethod() {
      return "result";
    }
  }

  @Test
  void registersBeansWithWorkflowMethods() {
    var mockDbos = mock(DBOS.class);
    var mockCtx = mock(ApplicationContext.class);
    var bean = new BeanWithWorkflow();

    when(mockCtx.getBeanDefinitionNames()).thenReturn(new String[] {"workflowBean"});
    when(mockCtx.getBean("workflowBean")).thenReturn(bean);

    new DBOSWorkflowRegistrar(mockDbos, mockCtx).afterSingletonsInstantiated();

    verify(mockDbos).registerClassWorkflows(bean, "");
  }

  @Test
  void skipsBeansWithoutWorkflowMethods() {
    var mockDbos = mock(DBOS.class);
    var mockCtx = mock(ApplicationContext.class);

    when(mockCtx.getBeanDefinitionNames()).thenReturn(new String[] {"plainBean"});
    when(mockCtx.getBean("plainBean")).thenReturn(new BeanWithoutWorkflow());

    new DBOSWorkflowRegistrar(mockDbos, mockCtx).afterSingletonsInstantiated();

    verify(mockDbos, never())
        .registerClassWorkflows(
            org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any());
  }

  @Test
  void skipsBeansThatThrowOnLookup() {
    var mockDbos = mock(DBOS.class);
    var mockCtx = mock(ApplicationContext.class);

    when(mockCtx.getBeanDefinitionNames()).thenReturn(new String[] {"badBean"});
    when(mockCtx.getBean("badBean")).thenThrow(new RuntimeException("bean not available"));

    // should complete without throwing
    new DBOSWorkflowRegistrar(mockDbos, mockCtx).afterSingletonsInstantiated();

    verify(mockDbos, never())
        .registerClassWorkflows(
            org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any());
  }

  @Test
  void processesMultipleBeans() {
    var mockDbos = mock(DBOS.class);
    var mockCtx = mock(ApplicationContext.class);
    var wfBean = new BeanWithWorkflow();
    var plainBean = new BeanWithoutWorkflow();

    when(mockCtx.getBeanDefinitionNames()).thenReturn(new String[] {"wfBean", "plainBean"});
    when(mockCtx.getBean("wfBean")).thenReturn(wfBean);
    when(mockCtx.getBean("plainBean")).thenReturn(plainBean);

    new DBOSWorkflowRegistrar(mockDbos, mockCtx).afterSingletonsInstantiated();

    verify(mockDbos).registerClassWorkflows(wfBean, "");
    verify(mockDbos, never()).registerClassWorkflows(plainBean, "");
  }
}
