
# 说明 
- 支持java 普通对象直接录入数据库,要求对象字段和数据库字段名称相同,暂不支持字段名称转换
- 支持常见JDBC协议,需要什么驱动包请自行下载,内部会自动加载驱动包
- 不支持JSON String, 但是可以将JSON String 转成Map
- 支持batchSize 和intervalMS攒批录入
- 支持Mysql,Oracle,Sqlserver,Postgresql,Sqlite,H2,Clickhouse,Bytehouse,StarRocks,ADB等所有支持JDBC协议的数据库
- 仅支持单表录入,不支持多表同时录入
# demo
**以bytehouse为例, demo如下**
```java
public class Demo {}

public class Main {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new MySource()).setParallelism(1).addSink(JdbcBatchSink.sink(
                JdbcConnectionCoreOptions.builder()
                        .withUrl("jdbc:bytehouse://tenant-2000007949-cn-shanghai.bytehouse.ivolces.com:19000?secure=true&database=venus")
                        .withUsername("bytehouse")
                        .withDriverName("com.bytedance.bytehouse.jdbc.ByteHouseDriver")
                        .withPassword("aAUvBmMEoo:eeLDmfk9Mt")
                        .withConnectionCheckTimeoutSeconds(60)
                        .build(),
                JdbcExecutionCoreOptions.builder()
                        .withTableName("Book")
                        .withBatchSize(50000)
                        .withBatchIntervalMs(2000)
                        .withMaxRetries(1)
                        .build()
        )).setParallelism(2);
        env.execute();


    }
}
```