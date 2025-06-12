package dev.dbos.transact.interceptor;

import dev.dbos.transact.workflow.Workflow;

public class OrderServiceImpl implements OrderService {

    @Override
    @Workflow(name = "processOrder")
    public String processOrder(String item) {
        return "Processed: " + item;
    }
}
