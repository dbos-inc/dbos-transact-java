package dev.dbos.transact.spring;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.workflow.Step;
import dev.dbos.transact.workflow.Workflow;

import org.junit.jupiter.api.Test;
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

  static class BeanWithStep {
    @Step
    public String myStep() {
      return "result";
    }
  }

  static class BeanWithWorkflowAndStep {
    @Workflow
    public String myWorkflow() {
      return "result";
    }

    @Step
    public String myStep() {
      return "step";
    }
  }

  static class BaseWorkflowBean {
    @Workflow
    public String baseWorkflow() {
      return "result";
    }
  }

  static class DerivedWorkflowBean extends BaseWorkflowBean {}

  private static ConfigurableApplicationContext mockCtx(
      ConfigurableListableBeanFactory beanFactory, String... beanNames) {
    var mockCtx = mock(ConfigurableApplicationContext.class);
    when(mockCtx.getBeanDefinitionNames()).thenReturn(beanNames);
    when(mockCtx.getBeanFactory()).thenReturn(beanFactory);
    when(mockCtx.isSingleton(any())).thenReturn(true);
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
  void beanWithBothWorkflowAndStep_onlyRegistersWorkflow() throws Exception {
    var mockDbos = mock(DBOS.class);
    var mockBeanFactory = mock(ConfigurableListableBeanFactory.class);
    var mockCtx = mockCtx(mockBeanFactory, "mixedBean");
    var bean = new BeanWithWorkflowAndStep();

    when(mockCtx.getBean("mixedBean")).thenReturn(bean);

    new DBOSWorkflowRegistrar(mockDbos, mockCtx).afterSingletonsInstantiated();

    var method = BeanWithWorkflowAndStep.class.getMethod("myWorkflow");
    verify(mockDbos).registerWorkflow(any(), eq(bean), eq(method), eq(null));
    verify(mockDbos, never())
        .registerWorkflow(
            any(), any(), eq(BeanWithWorkflowAndStep.class.getMethod("myStep")), any());
  }

  @Test
  void beanLookupFailureMidScan_doesNotPreventOtherBeanRegistration() {
    var mockDbos = mock(DBOS.class);
    var mockBeanFactory = mock(ConfigurableListableBeanFactory.class);
    var mockCtx = mockCtx(mockBeanFactory, "badBean", "goodBean");
    var goodBean = new BeanWithWorkflow();

    when(mockCtx.getBean("badBean")).thenThrow(new RuntimeException("not available"));
    when(mockCtx.getBean("goodBean")).thenReturn(goodBean);

    new DBOSWorkflowRegistrar(mockDbos, mockCtx).afterSingletonsInstantiated();

    verify(mockDbos).registerWorkflow(any(), eq(goodBean), any(), eq(null));
  }

  @Test
  void inheritedWorkflowMethods_areDetectedAndRegistered() throws Exception {
    var mockDbos = mock(DBOS.class);
    var mockBeanFactory = mock(ConfigurableListableBeanFactory.class);
    var mockCtx = mockCtx(mockBeanFactory, "derivedBean");
    var bean = new DerivedWorkflowBean();

    when(mockCtx.getBean("derivedBean")).thenReturn(bean);

    new DBOSWorkflowRegistrar(mockDbos, mockCtx).afterSingletonsInstantiated();

    var method = BaseWorkflowBean.class.getDeclaredMethod("baseWorkflow");
    verify(mockDbos).registerWorkflow(any(), eq(bean), eq(method), eq(null));
  }

  @Test
  void nonSingletonBeanWithWorkflow_throwsIllegalStateException() {
    var mockDbos = mock(DBOS.class);
    var mockBeanFactory = mock(ConfigurableListableBeanFactory.class);
    var mockCtx = mockCtx(mockBeanFactory, "prototypeBean");

    when(mockCtx.getBean("prototypeBean")).thenReturn(new BeanWithWorkflow());
    when(mockCtx.isSingleton("prototypeBean")).thenReturn(false);

    assertThrows(
        IllegalStateException.class,
        () -> new DBOSWorkflowRegistrar(mockDbos, mockCtx).afterSingletonsInstantiated());
  }

  @Test
  void nonSingletonBeanWithStep_throwsIllegalStateException() {
    var mockDbos = mock(DBOS.class);
    var mockBeanFactory = mock(ConfigurableListableBeanFactory.class);
    var mockCtx = mockCtx(mockBeanFactory, "prototypeBean");

    when(mockCtx.getBean("prototypeBean")).thenReturn(new BeanWithStep());
    when(mockCtx.isSingleton("prototypeBean")).thenReturn(false);

    assertThrows(
        IllegalStateException.class,
        () -> new DBOSWorkflowRegistrar(mockDbos, mockCtx).afterSingletonsInstantiated());
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
