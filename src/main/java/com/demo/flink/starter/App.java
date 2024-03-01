package com.demo.flink.starter;


import com.tpl.tpszjs.core.util.LoggerUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;

import java.util.ServiceLoader;
import java.util.Set;

import static com.tpl.tpszjs.flink.common.AppConstants.EXECUTE_ENV_KEY;

public class App {

    private static final Logger LOG = LoggerUtil.getLogger();

    public static void main(String[] args) throws IllegalAccessException, InstantiationException {
        final ParameterTool argsParameter = ParameterTool.fromArgs(args);
        String env = argsParameter.get(EXECUTE_ENV_KEY);
        String config = env.contains("/") ? env.substring(env.lastIndexOf("/") + 1) : env ;
        String configFile = config.endsWith(".properties") ? config : config + ".properties";
        LOG.info("load evn:{}", env);
        boolean foundRunner = false;
        ServiceLoader<AppRunner> loader = ServiceLoader.load(AppRunner.class);
        for (AppRunner implClass : loader) {
            final AppRunner appRunner = implClass.getClass().newInstance();
            Set<String> supportEnvConfs = appRunner.supportEnvConfs();
            if (supportEnvConfs.contains(configFile)) {
                LOG.info("found env runner:{}", implClass.getClass().getSimpleName());
                foundRunner = true;
                appRunner.execute(args);
                break;
            }
        }
        if (!foundRunner) {
            throw new IllegalArgumentException("Unsupported " + EXECUTE_ENV_KEY + ":" + env);
        }
    }
}
