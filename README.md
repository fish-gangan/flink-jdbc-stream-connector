# demo
```
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(...).setParallelism(1).addSink(JdbcBatchSink.sink(
                JdbcConnectionCoreOptions.builder()
                        .withUrl("url")
                        .withUsername("用户名")
                        .withDriverName("com....")
                        .withPassword("密码")
                        .withConnectionCheckTimeoutSeconds(60)
                        .build(),
                JdbcExecutionCoreOptions.builder()
                        .withTableName("表名")
                        .withBatchSize(50000)//批次大小
                        .withBatchIntervalMs(2000)//interval 间隔
                        .withMaxRetries(1)//录入失败时，尝试次数
                        .build()
        )).setParallelism(2);
        env.execute();

```
# 说明 
上面代码未涉及到Sql, 因为内部用了hutool-db , 可自动将任何Bean转成可插入数据库的对象
因此支持常见的用户自定义对象，目前暂不支持 纯JSON string, 但是可以将JSON STRING 转成Map
