package dev.dbos.transact.interceptor;

import dev.dbos.transact.workflow.Step;
import dev.dbos.transact.workflow.Transaction;
import dev.dbos.transact.workflow.Workflow;

public class OrderServiceImpl implements OrderService {

    @Override
    @Workflow(name = "processOrder")
    public String processOrder(String item) {
        return "Processed: " + item;
    }

    @Override
    @Step(name = "reserve")
    public String reserveInventory(String orderId, int itemId, int quantity) {
        return orderId + itemId + quantity ;
    }

    @Override
    @Transaction(name = "charge")
    public String chargeCustomer(String orderId, double amount) {
        return orderId+amount ;

    }
}
