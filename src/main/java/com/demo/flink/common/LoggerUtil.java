package com.demo.flink.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * @Author zhanggaoyu
 * @Date 2024/3/1
 * @Description
 */
public class LoggerUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerUtil.class);

    public static Logger getLogger() {
        try {
            StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
            if (stackTrace.length < 3 ||
                    !Objects.equals("getStackTrace", stackTrace[0].getMethodName()) ||
                    !Objects.equals("getLogger", stackTrace[1].getMethodName())) {
                return LOGGER;
            } else {
                String className = stackTrace[2].getClassName();
                String[] split = className.split("\\.");
                return LoggerFactory.getLogger(split[split.length - 1]);
            }
        } catch (Exception e) {
            LOGGER.error("error occurred while getLogger()", e);
            return LOGGER;
        }
    }

}
