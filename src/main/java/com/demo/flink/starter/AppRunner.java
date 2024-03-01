package com.demo.flink.starter;

import java.util.Set;

public interface AppRunner {

    Set<String> supportEnvConfs();

    void execute(String[] args);
}
