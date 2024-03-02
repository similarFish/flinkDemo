package com.demo.flink.core;

import com.demo.flink.common.LoggerUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.concurrent.TimeUnit;


public class ExecutionEnvBuilder {

    private static final Logger LOG = LoggerUtil.getLogger();

    // create parameter tool
    public static ParameterTool createParameterTool(final String[] args) throws IOException {
        LOG.info("======= start build ParameterTool =======");
        final ParameterTool argsParameter = ParameterTool.fromArgs(args);
        return ParameterTool.fromPropertiesFile(
                ExecutionEnvBuilder.class.getClassLoader().getResourceAsStream("application.properties"))
                .mergeWith(argsParameter)
                .mergeWith(ParameterTool.fromSystemProperties());
    }

    public static StreamExecutionEnvironment buildEnvironment(ParameterTool tool) {
        LOG.info("======= start build StreamExecutionEnvironment =======");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // set job global parameters and parallel
        env.getConfig().setGlobalJobParameters(tool);
        env.setParallelism(tool.getInt("stream.parallelism", 1));
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(60, Time.of(1, TimeUnit.MINUTES)));

        //config checkpoint
        boolean enableCheckpoint = tool.getBoolean("stream.checkpoint.enable", false);
        long checkpointInterval = tool.getLong("stream.checkpoint.interval.millisecond", 60000L);
        String checkpointPath = tool.get("stream.checkpoint.path", "");
        if (enableCheckpoint) {
            LOG.info("======= enable and start checkpoint =======");
            env.enableCheckpointing(checkpointInterval);
            CheckpointConfig config = env.getCheckpointConfig();
            config.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
            long timeout = tool.getLong("stream.checkpoint.timeout", 180000L);
            long minPause = tool.getLong("stream.checkpoint.min.pause", 30000L);
            config.setMinPauseBetweenCheckpoints(minPause);
            config.setCheckpointTimeout(timeout);
            config.setMaxConcurrentCheckpoints(1);
            config.setTolerableCheckpointFailureNumber(3);
            if (StringUtils.isBlank(checkpointPath)) {
                LOG.debug("checkpoint路径为空.");
            } else {
                env.getCheckpointConfig().setCheckpointStorage(checkpointPath);
            }
            // default event time
            LOG.info("======= ignore set checkpointPath, please specified state.checkpoints.dir path by using terminal shell =======");
        } else {
            LOG.info("======= not open checkpoint =======");
        }
        return env;
    }
}
