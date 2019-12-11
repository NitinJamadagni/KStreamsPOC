package com.visa.ip.settle.poc.kstreams;

public class ApplicationTrigger {

    public static void main(String[] args) throws InterruptedException {

        AppRunner appRunner = new AppRunner();
        appRunner.setup();
        appRunner.run();

    }
}
