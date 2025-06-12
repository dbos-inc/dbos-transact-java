package dev.dbos.transact.interceptor;

public interface OrderService {

    String processOrder(String item) ;

    String reserveInventory(String orderId, int itemId, int quantity);

    String chargeCustomer(String orderId, double amount);
}
