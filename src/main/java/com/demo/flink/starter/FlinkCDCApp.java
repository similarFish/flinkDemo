package com.demo.flink.starter;

import com.tpl.tpszjs.core.util.LoggerUtil;
import com.tpl.tpszjs.flink.core.ExecutionEnvBuilder;
import com.tpl.tpszjs.flink.opr.CDCUpdateTimeWatermarkGeneratorSupplier;
import com.tpl.tpszjs.flink.opr.MyDeserializationSchemaFunction;
import com.tpl.tpszjs.flink.sink.KafkaSinkBuilder;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.commons.compress.utils.Sets;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;

import java.util.Properties;
import java.util.Set;

public class FlinkCDCApp implements AppRunner {

    private static final Logger LOG = LoggerUtil.getLogger();


    @Override
    public Set<String> supportEnvConfs() {
        return Sets.newHashSet("application-flinkcdc-prd.properties", "application-flinkcdc-dev.properties");
    }

    @Override
    public void execute(String[] args) {
        try {
            LOG.info("start application flinkCDC");
            //TODO 1.基础环境
            //1.1流处理执行环境

            ParameterTool tool = ExecutionEnvBuilder.createParameterTool(args);

            final StreamExecutionEnvironment env = ExecutionEnvBuilder.buildEnvironment(tool);
            //1.2设置并行度
            env.setParallelism(tool.getInt("stream.parallelism"));//设置并行度为1方便测试

            //TODO 2.检查点配置
            //2.1 开启检查点
            env.enableCheckpointing(30000L, CheckpointingMode.EXACTLY_ONCE);//5秒执行一次，模式：精准一次性
            //2.2 设置检查点超时时间
            env.getCheckpointConfig().setCheckpointTimeout(60*1000);
            //2.3 设置重启策略
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 2*1000));//两次，两秒执行一次
            //2.4 设置job取消后检查点是否保留
            env.getCheckpointConfig().enableExternalizedCheckpoints(
                    CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);//保留
            //2.5 设置状态后端-->保存到hdfs 10.28.133.72
            //file:///tmp/flinkops/zgytest/flinkcdctest/checkpoint/
            env.setStateBackend(new FsStateBackend(tool.get("stream.checkpoint.path")));

            //TODO 3.FlinkCDC
            //3.1 创建MySQLSource
            Properties properties = new Properties();
            properties.put("snapshot.locking.mode", "none");
            properties.setProperty("converters", "dateConverters");
            properties.setProperty("dateConverters.type", "com.tpl.tpszjs.flink.common.MySqlDateTimeConverter");


            SourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                    .hostname(tool.get("mysql.host"))
                    .port(tool.getInt("mysql.port"))
                    .databaseList(tool.get("mysql.database"))//库
                    .tableList(tool.get("mysql.tablelist"))//表
                    .username(tool.get("mysql.username"))
                    .password(tool.get("mysql.password"))
                    .serverTimeZone("Asia/Shanghai")
                    .debeziumProperties(properties)
                    .startupOptions(StartupOptions.latest())//启动的时候从第一次开始读取
                    .deserializer(new MyDeserializationSchemaFunction())//这里使用自定义的反序列化器将数据封装成json格式
                    .build();


            //3.2 从源端获取数据
            DataStreamSource<String> sourceDS = env.addSource(sourceFunction);
            SingleOutputStreamOperator cdcStream = sourceDS.assignTimestampsAndWatermarks(WatermarkStrategy.forGenerator(
                    new CDCUpdateTimeWatermarkGeneratorSupplier(1000 * 60)));

            //打印测试
            cdcStream.print();
            //3.打印数据并将数据写入kafka
            cdcStream.sinkTo(KafkaSinkBuilder.buildProducer(tool));

            //执行
            env.execute("FlinkCDCTest");

        } catch (Exception e) {
            LOG.error("job failed.", e);
            System.out.println("job failed." + e.getMessage());
            e.printStackTrace();
        }
    }
}
