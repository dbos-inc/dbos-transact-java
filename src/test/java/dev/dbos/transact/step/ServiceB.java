package dev.dbos.transact.step;

public interface ServiceB {

    public String step1(String input) ;
    public String step2(String input) ;
    public String step3(String input, boolean throwError) throws Exception;
    public String step4(String input) ;
    public String step5(String input) ;

}
