package dev.dbos.transact.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.execution.ThrowingSupplier;
import dev.dbos.transact.internal.DBOSIntegration;
import dev.dbos.transact.workflow.Step;
import dev.dbos.transact.workflow.StepOptions;
import dev.dbos.transact.workflow.Workflow;

import java.lang.reflect.Method;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.ConfigurableApplicationContext;

class DBOSAspectTest {

  static class MyBean {
    @Workflow
    public String myWorkflow() {
      return "workflow-result";
    }

    @Step
    public String myStep() {
      return "step-result";
    }
  }

  private DBOS mockDbos;
  private DBOSIntegration mockIntegration;
  private ConfigurableApplicationContext mockCtx;
  private ConfigurableListableBeanFactory mockBeanFactory;
  private DBOSAspect aspect;

  private Method workflowMethod;
  private Method stepMethod;
  private Workflow workflowAnnotation;
  private Step stepAnnotation;

  private ProceedingJoinPoint pjp;
  private MethodSignature sig;

  @BeforeEach
  void setUp() throws NoSuchMethodException {
    workflowMethod = MyBean.class.getDeclaredMethod("myWorkflow");
    stepMethod = MyBean.class.getDeclaredMethod("myStep");
    workflowAnnotation = workflowMethod.getAnnotation(Workflow.class);
    stepAnnotation = stepMethod.getAnnotation(Step.class);

    mockDbos = mock(DBOS.class);
    mockIntegration = mock(DBOSIntegration.class);
    mockCtx = mock(ConfigurableApplicationContext.class);
    mockBeanFactory = mock(ConfigurableListableBeanFactory.class);

    when(mockDbos.integration()).thenReturn(mockIntegration);
    when(mockCtx.getBeanFactory()).thenReturn(mockBeanFactory);

    aspect = new DBOSAspect(mockDbos, mockCtx);

    pjp = mock(ProceedingJoinPoint.class);
    sig = mock(MethodSignature.class);
    when(pjp.getSignature()).thenReturn(sig);
    when(pjp.getArgs()).thenReturn(new Object[] {});
  }

  private void setupWorkflowPjp(Object target) {
    when(pjp.getTarget()).thenReturn(target);
    when(sig.getMethod()).thenReturn(workflowMethod);
    when(sig.getName()).thenReturn("myWorkflow");
  }

  private void setupStepPjp(Object target) {
    when(pjp.getTarget()).thenReturn(target);
    when(sig.getMethod()).thenReturn(stepMethod);
    when(sig.getName()).thenReturn("myStep");
  }

  @SuppressWarnings("unchecked")
  private void mockRunStepInvokesSupplier() throws Exception {
    Mockito.doAnswer(inv -> ((ThrowingSupplier<Object, Exception>) inv.getArgument(0)).execute())
        .when(mockDbos)
        .runStep(Mockito.<ThrowingSupplier<Object, Exception>>any(), any(StepOptions.class));
  }

  // --- aroundWorkflow ---

  @Test
  void aroundWorkflow_delegatesToDbosIntegration() throws Throwable {
    var target = new MyBean();
    setupWorkflowPjp(target);
    when(mockCtx.getBeanNamesForType(MyBean.class)).thenReturn(new String[] {"myBean"});
    when(mockIntegration.runWorkflow(any(), any(), any(), any(), any())).thenReturn("result");

    Object result = aspect.aroundWorkflow(pjp, workflowAnnotation);

    assertThat(result).isEqualTo("result");
    verify(mockIntegration)
        .runWorkflow(eq(target), eq(""), eq(workflowMethod), any(), eq(workflowAnnotation));
  }

  // --- resolveInstanceName via aroundWorkflow ---

  @Test
  void aroundWorkflow_singleBean_resolvesEmptyInstanceName() throws Throwable {
    var target = new MyBean();
    setupWorkflowPjp(target);
    when(mockCtx.getBeanNamesForType(MyBean.class)).thenReturn(new String[] {"myBean"});
    when(mockIntegration.runWorkflow(any(), any(), any(), any(), any())).thenReturn(null);

    aspect.aroundWorkflow(pjp, workflowAnnotation);

    var captor = ArgumentCaptor.forClass(String.class);
    verify(mockIntegration).runWorkflow(any(), captor.capture(), any(), any(), any());
    assertThat(captor.getValue()).isEqualTo("");
  }

  @Test
  void aroundWorkflow_multipleBeans_primaryResolvesEmptyInstanceName() throws Throwable {
    var primaryTarget = new MyBean();
    var secondaryTarget = new MyBean();
    setupWorkflowPjp(primaryTarget);
    when(mockCtx.getBeanNamesForType(MyBean.class))
        .thenReturn(new String[] {"primaryBean", "secondaryBean"});
    when(mockCtx.getBean("primaryBean")).thenReturn(primaryTarget);
    when(mockCtx.getBean("secondaryBean")).thenReturn(secondaryTarget);
    var primaryDef = mock(BeanDefinition.class);
    when(primaryDef.isPrimary()).thenReturn(true);
    when(mockBeanFactory.getBeanDefinition("primaryBean")).thenReturn(primaryDef);
    when(mockIntegration.runWorkflow(any(), any(), any(), any(), any())).thenReturn(null);

    aspect.aroundWorkflow(pjp, workflowAnnotation);

    var captor = ArgumentCaptor.forClass(String.class);
    verify(mockIntegration).runWorkflow(any(), captor.capture(), any(), any(), any());
    assertThat(captor.getValue()).isEqualTo("");
  }

  @Test
  void aroundWorkflow_multipleBeans_nonPrimaryResolvesAsBeanName() throws Throwable {
    var primaryTarget = new MyBean();
    var secondaryTarget = new MyBean();
    setupWorkflowPjp(secondaryTarget);
    when(mockCtx.getBeanNamesForType(MyBean.class))
        .thenReturn(new String[] {"primaryBean", "secondaryBean"});
    when(mockCtx.getBean("primaryBean")).thenReturn(primaryTarget);
    when(mockCtx.getBean("secondaryBean")).thenReturn(secondaryTarget);
    var secondaryDef = mock(BeanDefinition.class);
    when(secondaryDef.isPrimary()).thenReturn(false);
    when(mockBeanFactory.getBeanDefinition("secondaryBean")).thenReturn(secondaryDef);
    when(mockIntegration.runWorkflow(any(), any(), any(), any(), any())).thenReturn(null);

    aspect.aroundWorkflow(pjp, workflowAnnotation);

    var captor = ArgumentCaptor.forClass(String.class);
    verify(mockIntegration).runWorkflow(any(), captor.capture(), any(), any(), any());
    assertThat(captor.getValue()).isEqualTo("secondaryBean");
  }

  @Test
  void aroundWorkflow_multipleBeans_noMatchingTargetResolvesEmpty() throws Throwable {
    var target = new MyBean(); // not registered in context
    setupWorkflowPjp(target);
    when(mockCtx.getBeanNamesForType(MyBean.class)).thenReturn(new String[] {"beanA", "beanB"});
    when(mockCtx.getBean("beanA")).thenReturn(new MyBean());
    when(mockCtx.getBean("beanB")).thenReturn(new MyBean());
    when(mockIntegration.runWorkflow(any(), any(), any(), any(), any())).thenReturn(null);

    aspect.aroundWorkflow(pjp, workflowAnnotation);

    var captor = ArgumentCaptor.forClass(String.class);
    verify(mockIntegration).runWorkflow(any(), captor.capture(), any(), any(), any());
    assertThat(captor.getValue()).isEqualTo("");
  }

  @Test
  void aroundWorkflow_multipleInstances_routesWithCorrectInstanceName() throws Throwable {
    var primaryTarget = new MyBean();
    var secondaryTarget = new MyBean();
    when(mockCtx.getBeanNamesForType(MyBean.class))
        .thenReturn(new String[] {"primaryBean", "secondaryBean"});
    when(mockCtx.getBean("primaryBean")).thenReturn(primaryTarget);
    when(mockCtx.getBean("secondaryBean")).thenReturn(secondaryTarget);
    var primaryDef = mock(BeanDefinition.class);
    when(primaryDef.isPrimary()).thenReturn(true);
    when(mockBeanFactory.getBeanDefinition("primaryBean")).thenReturn(primaryDef);
    var secondaryDef = mock(BeanDefinition.class);
    when(secondaryDef.isPrimary()).thenReturn(false);
    when(mockBeanFactory.getBeanDefinition("secondaryBean")).thenReturn(secondaryDef);
    when(mockIntegration.runWorkflow(any(), any(), any(), any(), any())).thenReturn(null);

    when(pjp.getTarget()).thenReturn(primaryTarget);
    when(sig.getMethod()).thenReturn(workflowMethod);
    aspect.aroundWorkflow(pjp, workflowAnnotation);

    when(pjp.getTarget()).thenReturn(secondaryTarget);
    aspect.aroundWorkflow(pjp, workflowAnnotation);

    var captor = ArgumentCaptor.forClass(String.class);
    verify(mockIntegration, times(2)).runWorkflow(any(), captor.capture(), any(), any(), any());
    assertThat(captor.getAllValues()).containsExactly("", "secondaryBean");
  }

  // --- aroundStep ---

  @Test
  void aroundStep_invokesSupplierAndReturnsResult() throws Throwable {
    setupStepPjp(new MyBean());
    when(pjp.proceed()).thenReturn("step-result");
    mockRunStepInvokesSupplier();

    Object result = aspect.aroundStep(pjp, stepAnnotation);

    assertThat(result).isEqualTo("step-result");
  }

  @Test
  void aroundStep_wrapsNonExceptionThrowablesAndUnwraps() throws Throwable {
    setupStepPjp(new MyBean());
    var oom = new OutOfMemoryError("oom");
    when(pjp.proceed()).thenThrow(oom);
    mockRunStepInvokesSupplier();

    var thrown = assertThrows(OutOfMemoryError.class, () -> aspect.aroundStep(pjp, stepAnnotation));
    assertSame(oom, thrown);
  }

  @Test
  void aroundStep_rethrowsExceptions() throws Throwable {
    setupStepPjp(new MyBean());
    var ex = new RuntimeException("test");
    when(pjp.proceed()).thenThrow(ex);
    mockRunStepInvokesSupplier();

    var thrown = assertThrows(RuntimeException.class, () -> aspect.aroundStep(pjp, stepAnnotation));
    assertSame(ex, thrown);
  }

  @Test
  void aroundStep_unwrapsWrappedThrowableFromRunStep() throws Throwable {
    setupStepPjp(new MyBean());
    var oom = new OutOfMemoryError("oom");
    Mockito.doThrow(new WrappedThrowableException(oom))
        .when(mockDbos)
        .runStep(Mockito.<ThrowingSupplier<Object, Exception>>any(), any(StepOptions.class));

    var thrown = assertThrows(OutOfMemoryError.class, () -> aspect.aroundStep(pjp, stepAnnotation));
    assertSame(oom, thrown);
  }

  @Test
  void aroundStep_cachesSameStepOptionsForSameMethod() throws Throwable {
    setupStepPjp(new MyBean());
    when(pjp.proceed()).thenReturn("result");
    mockRunStepInvokesSupplier();

    aspect.aroundStep(pjp, stepAnnotation);
    aspect.aroundStep(pjp, stepAnnotation);

    var captor = ArgumentCaptor.forClass(StepOptions.class);
    verify(mockDbos, times(2))
        .runStep(Mockito.<ThrowingSupplier<Object, Exception>>any(), captor.capture());
    assertSame(captor.getAllValues().get(0), captor.getAllValues().get(1));
  }
}
