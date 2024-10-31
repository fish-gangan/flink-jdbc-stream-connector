# 依赖
```pom
    <repositories>
        <repository>
            <id>apache.snapshots</id>
            <name>Apache Development Snapshot Repository</name>
            <url>https://repository.apache.org/content/repositories/snapshots/</url>
            <releases>
                <enabled>false</enabled>
            </releases>
            <snapshots>
                <enabled>true</enabled>
            </snapshots>
        </repository>
        <repository>
            <!--火山引擎bytehouse 仓库地址-->
            <id>bytedance</id>
            <name>ByteDance Public Repository</name>
            <url>https://artifact.bytedance.com/repository/releases</url>
        </repository>
    </repositories>

</dependencies>
     <dependency>
        <!--用于bean到可插入sql对象Entity的转换-->
         <groupId>cn.hutool</groupId>
         <artifactId>hutool-db</artifactId>
         <version>5.8.16</version>
    </dependency>
 
     <dependency>
         <groupId>com.google.guava</groupId>
         <artifactId>guava</artifactId>
         <version>30.1.1-jre</version>
     </dependency>
     
     <dependency>
        <!-- bytehouse 还需要以下配置 -->
        <groupId>com.bytedance.bytehouse</groupId>
        <artifactId>driver-java</artifactId>
        <version>1.1.58</version>
        <classifier>all</classifier>
     </dependency>
</dependencies>
 




```

# 说明 
- 支持java 普通对象直接录入数据库,要求对象字段和数据库字段名称相同,暂不支持字段名称转换
  
- 支持常见JDBC协议,需要什么驱动包请自行下载,内部会自动加载驱动包
  
- 支持batchSize 和intervalMS攒批录入
  
- 支持Mysql,Oracle,Sqlserver,Postgresql,
  Sqlite,H2,Clickhouse,Bytehouse,StarRocks,ADB等所有支持JDBC协议的数据库
  
- 仅支持单表录入,不支持多表同时录入
  
- 不支持JSON String, 但支持Map

- 不支持FlinkSql 语法
# demo
**以bytehouse为例, demo如下**
```java
public class Demo {}

public class Main {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new MySource()).setParallelism(1).addSink(JdbcBatchSink.sink(
                JdbcConnectionCoreOptions.builder()
                        .withUrl("jdbc:bytehouse://ip:19000?secure=true&database=库名")
                        .withUsername("bytehouse")
                        .withDriverName("com.bytedance.bytehouse.jdbc.ByteHouseDriver")
                        .withPassword("password")
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
