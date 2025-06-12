package dev.dbos.transact;

import dev.dbos.transact.interceptor.TransactInvocationHandler;

public class DBOS {

    public static <T> WorkflowBuilder<T> Workflow() {
        return new WorkflowBuilder<>();
    }

    // Optional static inner builder class for workflows
    public static class WorkflowBuilder<T> {
        private Class<T> interfaceClass;
        private T implementation;


        public WorkflowBuilder<T> interfaceClass(Class<T> iface) {
            this.interfaceClass = iface;
            return this;
        }

        public WorkflowBuilder<T> implementation(T impl) {
            this.implementation = impl;
            return this;
        }



        public T build() {
            if (interfaceClass == null || implementation == null) {
                throw new IllegalStateException("Interface and implementation must be set");
            }

            return TransactInvocationHandler.createProxy(
                    interfaceClass,
                    implementation
            );
        }
    }
}

