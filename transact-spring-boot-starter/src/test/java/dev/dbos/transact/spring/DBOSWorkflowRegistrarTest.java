package dev.dbos.transact.spring;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.workflow.Workflow;

import org.junit.jupiter.api.Test;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.ConfigurableApplicationContext;

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

  private static ConfigurableApplicationContext mockCtx(
      ConfigurableListableBeanFactory beanFactory, String... beanNames) {
    var mockCtx = mock(ConfigurableApplicationContext.class);
    when(mockCtx.getBeanDefinitionNames()).thenReturn(beanNames);
    when(mockCtx.getBeanFactory()).thenReturn(beanFactory);
    return mockCtx;
  }

  @Test
  void registersBeansWithWorkflowMethods() throws Exception {
    var mockDbos = mock(DBOS.class);
    var mockBeanFactory = mock(ConfigurableListableBeanFactory.class);
    var mockCtx = mockCtx(mockBeanFactory, "workflowBean");
    var bean = new BeanWithWorkflow();

    when(mockCtx.getBean("workflowBean")).thenReturn(bean);

    new DBOSWorkflowRegistrar(mockDbos, mockCtx).afterSingletonsInstantiated();

    var method = BeanWithWorkflow.class.getMethod("myWorkflow");
    assertNotNull(method);
    var wfTag = method.getAnnotation(Workflow.class);
    assertNotNull(wfTag);
    
    verify(mockDbos).registerWorkflow(eq(wfTag), eq(bean), eq(method), eq(null));
  }

  @Test
  void skipsBeansWithoutWorkflowMethods() {
    var mockDbos = mock(DBOS.class);
    var mockBeanFactory = mock(ConfigurableListableBeanFactory.class);
    var mockCtx = mockCtx(mockBeanFactory, "plainBean");

    when(mockCtx.getBean("plainBean")).thenReturn(new BeanWithoutWorkflow());

    new DBOSWorkflowRegistrar(mockDbos, mockCtx).afterSingletonsInstantiated();

    verify(mockDbos, never()).registerWorkflow(any(), any(), any(), any());
  }

  @Test
  void skipsBeansThatThrowOnLookup() {
    var mockDbos = mock(DBOS.class);
    var mockBeanFactory = mock(ConfigurableListableBeanFactory.class);
    var mockCtx = mockCtx(mockBeanFactory, "badBean");

    when(mockCtx.getBean("badBean")).thenThrow(new RuntimeException("bean not available"));

    // should complete without throwing
    new DBOSWorkflowRegistrar(mockDbos, mockCtx).afterSingletonsInstantiated();

    verify(mockDbos, never()).registerWorkflow(any(), any(), any(), any());
  }

  @Test
  void processesMultipleBeans() {
    var mockDbos = mock(DBOS.class);
    var mockBeanFactory = mock(ConfigurableListableBeanFactory.class);
    var mockCtx = mockCtx(mockBeanFactory, "wfBean", "plainBean");
    var wfBean = new BeanWithWorkflow();
    var plainBean = new BeanWithoutWorkflow();

    when(mockCtx.getBean("wfBean")).thenReturn(wfBean);
    when(mockCtx.getBean("plainBean")).thenReturn(plainBean);

    new DBOSWorkflowRegistrar(mockDbos, mockCtx).afterSingletonsInstantiated();

    verify(mockDbos).registerWorkflow(any(), eq(wfBean), any(), eq(null));
    verify(mockDbos, never()).registerWorkflow(any(), eq(plainBean), any(), any());
  }

  @Test
  void registersMultipleBeansOfSameClassUsingBeanNames() {
    var mockDbos = mock(DBOS.class);
    var mockBeanFactory = mock(ConfigurableListableBeanFactory.class);
    var mockCtx = mockCtx(mockBeanFactory, "primaryBean", "secondaryBean");
    var primaryBean = new BeanWithWorkflow();
    var secondaryBean = new BeanWithWorkflow();

    when(mockCtx.getBean("primaryBean")).thenReturn(primaryBean);
    when(mockCtx.getBean("secondaryBean")).thenReturn(secondaryBean);

    var primaryDef = mock(BeanDefinition.class);
    var secondaryDef = mock(BeanDefinition.class);
    when(mockBeanFactory.getBeanDefinition("primaryBean")).thenReturn(primaryDef);
    when(mockBeanFactory.getBeanDefinition("secondaryBean")).thenReturn(secondaryDef);
    when(primaryDef.isPrimary()).thenReturn(true);
    when(secondaryDef.isPrimary()).thenReturn(false);

    new DBOSWorkflowRegistrar(mockDbos, mockCtx).afterSingletonsInstantiated();

    verify(mockDbos).registerWorkflow(any(), eq(primaryBean), any(), eq(null));
    verify(mockDbos).registerWorkflow(any(), eq(secondaryBean), any(), eq("secondaryBean"));
  }

  @Test
  void registersMultipleBeansOfSameClassWithBeanNamesWhenNoneIsPrimary() {
    var mockDbos = mock(DBOS.class);
    var mockBeanFactory = mock(ConfigurableListableBeanFactory.class);
    var mockCtx = mockCtx(mockBeanFactory, "beanA", "beanB");
    var beanA = new BeanWithWorkflow();
    var beanB = new BeanWithWorkflow();

    when(mockCtx.getBean("beanA")).thenReturn(beanA);
    when(mockCtx.getBean("beanB")).thenReturn(beanB);

    var defA = mock(BeanDefinition.class);
    var defB = mock(BeanDefinition.class);
    when(mockBeanFactory.getBeanDefinition("beanA")).thenReturn(defA);
    when(mockBeanFactory.getBeanDefinition("beanB")).thenReturn(defB);
    when(defA.isPrimary()).thenReturn(false);
    when(defB.isPrimary()).thenReturn(false);

    new DBOSWorkflowRegistrar(mockDbos, mockCtx).afterSingletonsInstantiated();

    verify(mockDbos).registerWorkflow(any(), eq(beanA), any(), eq("beanA"));
    verify(mockDbos).registerWorkflow(any(), eq(beanB), any(), eq("beanB"));
  }
}
