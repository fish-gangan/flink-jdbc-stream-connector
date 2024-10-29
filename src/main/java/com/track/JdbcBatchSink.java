package com.track;

import cn.hutool.db.DbUtil;
import cn.hutool.db.Entity;
import cn.hutool.db.GlobalDbConfig;
import cn.hutool.db.SqlConnRunner;
import com.google.common.collect.Sets;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.function.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Flushable;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;


/*
* 
* JDBC 连接协议的参数封装
* 主要包括：batchSize  intervalMS MAX_RETRY_TIMES
* 
* */
class JdbcExecutionCoreOptions implements Serializable {
    public static final int DEFAULT_TRIES = 3;
    private static final int DEFAULT_INTERVAL_MS = 0;
    public static final int DEFAULT_SIZE = 5000;
    public static final String DEFAULT_TABLE = "";

    private final long intervalMS;
    private final int batchSize;
    private final int tries;
    private final String tableName;

    private JdbcExecutionCoreOptions(long intervalMS, int batchSize, int tries, String tableName) {
        Preconditions.checkArgument(tries >= 0);
        this.intervalMS = intervalMS;
        this.batchSize = batchSize;
        this.tries = tries;
        this.tableName = tableName;
    }

    public long getBatchIntervalMs() {
        return intervalMS;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getMaxRetries() {
        return tries;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JdbcExecutionCoreOptions that = (JdbcExecutionCoreOptions) o;
        return intervalMS == that.intervalMS
                && batchSize == that.batchSize
                && tableName.equals(that.tableName)
                && tries == that.tries;
    }

    @Override
    public int hashCode() {
        return Objects.hash(intervalMS, batchSize, tries, tableName);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static JdbcExecutionCoreOptions defaults() {
        return builder().build();
    }

    /** Builder for {@link JdbcExecutionCoreOptions}. */
    @PublicEvolving
    public static final class Builder {
        private long intervalMs = DEFAULT_INTERVAL_MS;
        private int size = DEFAULT_SIZE;
        private int tries = DEFAULT_TRIES;
        private String tableName = DEFAULT_TABLE;


        public Builder withBatchSize(int size) {
            this.size = size;
            return this;
        }

        public Builder withBatchIntervalMs(long intervalMs) {
            this.intervalMs = intervalMs;
            return this;
        }

        public Builder withMaxRetries(int tries) {
            this.tries = tries;
            return this;
        }
        public Builder withTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public JdbcExecutionCoreOptions build() {
            return new JdbcExecutionCoreOptions(intervalMs, size, tries, tableName);
        }
    }
}

class JdbcConnectionCoreOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    protected final String url;
    @Nullable
    protected final String driverName;
    protected final int connectionCheckTimeoutSeconds;
    @Nullable protected final String username;
    @Nullable protected final String password;

    protected JdbcConnectionCoreOptions(
            String url,
            @Nullable String driverName,
            @Nullable String username,
            @Nullable String password,
            int connectionCheckTimeoutSeconds) {
        Preconditions.checkArgument(connectionCheckTimeoutSeconds > 0);
        this.url = Preconditions.checkNotNull(url, "jdbc url is empty");
        this.driverName = driverName;
        this.username = username;
        this.password = password;
        this.connectionCheckTimeoutSeconds = connectionCheckTimeoutSeconds;
    }

    public String getDbURL() {
        return url;
    }

    @Nullable
    public String getDriverName() {
        return driverName;
    }

    public Optional<String> getUsername() {
        return Optional.ofNullable(username);
    }

    public Optional<String> getPassword() {
        return Optional.ofNullable(password);
    }

    public int getConnectionCheckTimeoutSeconds() {
        return connectionCheckTimeoutSeconds;
    }
    public static JdbcConnectionOptionsBuilder builder() {
        return new JdbcConnectionOptionsBuilder();
    }

    /** Builder for {@link JdbcConnectionCoreOptions}. */
    public static class JdbcConnectionOptionsBuilder {
        private String url;
        private String driverName;
        private String username;
        private String password;
        private int connectionCheckTimeoutSeconds = 60;

        public JdbcConnectionOptionsBuilder withUrl(String url) {
            this.url = url;
            return this;
        }

        public JdbcConnectionOptionsBuilder withDriverName(String driverName) {
            this.driverName = driverName;
            return this;
        }

        public JdbcConnectionOptionsBuilder withUsername(String username) {
            this.username = username;
            return this;
        }

        public JdbcConnectionOptionsBuilder withPassword(String password) {
            this.password = password;
            return this;
        }

        /**
         * Set the maximum timeout between retries, default is 60 seconds.
         *
         * @param connectionCheckTimeoutSeconds the timeout seconds, shouldn't smaller than 1
         *     second.
         */
        public JdbcConnectionOptionsBuilder withConnectionCheckTimeoutSeconds(int connectionCheckTimeoutSeconds) {
            this.connectionCheckTimeoutSeconds = connectionCheckTimeoutSeconds;
            return this;
        }

        public JdbcConnectionCoreOptions build() {
            return new JdbcConnectionCoreOptions(
                    url, driverName, username, password, connectionCheckTimeoutSeconds);
        }
    }
}
/*
*
* 执行器接口
* */
interface JdbcBatchExecutor<R> {


    void prepareStatements(Connection connection) throws SQLException;

    /*
    * 单挑记录放入缓存等待输出
    * */
    void addToBuffer(R record) throws SQLException;

    /*
    * 执行批量输出
    * */
    void executeBatch() throws SQLException;


    void closeStatements() throws SQLException;

    /*
    * 一个简单的实现
    * */
    static <R> JdbcBatchExecutor<R> simple(String tableName) {
        return new SimpleJdbcBatchExecutor<>(tableName);
    }
}
/*
* 
* 执行器
* */
class SimpleJdbcBatchExecutor<R> implements JdbcBatchExecutor<R> {
    private final List<R> buffer;
    private SqlConnRunner sqlConnRunner;
    private Connection connection;
    private final String tableName;

     static{
        GlobalDbConfig.setReturnGeneratedKey(false);

//        GlobalDbConfig.setShowSql(true,true,true, Level.INFO);
    }
    SimpleJdbcBatchExecutor(String tableName) {
        this.buffer = new ArrayList<>();
        this.tableName = tableName;
    }

    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        this.sqlConnRunner = DbUtil.newSqlConnRunner(connection);
        this.connection = connection;

    }

    @Override
    public void addToBuffer(R record) throws SQLException {
        //加入缓存
        buffer.add(record);
    }


    private List<Entity> formatBuffers(){
        Set<String> allKeys = Sets.newHashSet();
        // buffer中的对象转成Entity对象
        List<Entity> entities= new ArrayList<>();
        for(R r:buffer){
            Entity entity =  Entity.parse(r);
            entity.setTableName(this.tableName);
            allKeys.addAll(entity.keySet());
            entities.add(entity);
        }


        entities.forEach(entity->{
            // 填充每个对象缺失的key
            Set<String> ob_k =  entity.keySet();
            Sets.SetView<String> distinctKeys = Sets.difference(allKeys,ob_k);
            distinctKeys.forEach(k->{
                entity.put(k,null);
            });
        });
        return entities;

    }

    @Override
    public void executeBatch() throws SQLException {
        List<Entity> entities =formatBuffers();
        sqlConnRunner.insert(connection, entities);
        buffer.clear();
    }

    @Override
    public void closeStatements() throws SQLException {

    }
}

class ConnectionProvider implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectionProvider.class);

    private static final long serialVersionUID = 1L;

    private final JdbcConnectionCoreOptions jdbcOptions;

    private transient Driver loadedDriver;
    private transient Connection connection;

    static {
        // Load DriverManager first to avoid deadlock between DriverManager's
        // static initialization block and specific driver class's static
        // initialization block when two different driver classes are loading
        // concurrently using Class.forName while DriverManager is uninitialized
        // before.
        //
        // This could happen in JDK 8 but not above as driver loading has been
        // moved out of DriverManager's static initialization block since JDK 9.
        DriverManager.getDrivers();
    }

    public ConnectionProvider(JdbcConnectionCoreOptions jdbcOptions) {
        this.jdbcOptions = jdbcOptions;
    }


    public Connection getConnection() {
        return connection;
    }


    public boolean isConnectionValid() throws SQLException {
        return connection != null && connection.isValid(jdbcOptions.getConnectionCheckTimeoutSeconds());
    }


    private Driver getDriver() throws SQLException, ClassNotFoundException {
        if (loadedDriver == null) {
//            loadedDriver = loadDriver(jdbcOptions.getDriverName());
            String driverName = jdbcOptions.getDriverName();
            Preconditions.checkNotNull(driverName);
            Enumeration<Driver> drivers = DriverManager.getDrivers();
            while (drivers.hasMoreElements()) {
                Driver driver = drivers.nextElement();
                if (driver.getClass().getName().equals(driverName)) {
                    loadedDriver = driver;
                }
            }
            // We could reach here for reasons:
            // * Class loader hell of DriverManager(see JDK-8146872).
            // * driver is not installed as a service provider.
            Class<?> clazz =  Class.forName(driverName, true, Thread.currentThread().getContextClassLoader());
            try {
                loadedDriver =  (Driver) clazz.newInstance();
            } catch (Exception ex) {
                throw new SQLException("Fail to create driver of class " + driverName, ex);
            }
        }
        return loadedDriver;

    }

    /*
    * 1. 存在connection直接返回
    * 2. 不存在则重建一个connection
    * */
    public Connection getOrEstablishConnection() throws SQLException, ClassNotFoundException {
        if (connection != null) {
            return connection;
        }
        if (jdbcOptions.getDriverName() == null) {
            connection =  DriverManager.getConnection(jdbcOptions.getDbURL(),
                            jdbcOptions.getUsername().orElse(null),
                            jdbcOptions.getPassword().orElse(null)
                         );
        } else {
            Driver driver = getDriver();
            Properties info = new Properties();
            info.setProperty("secure", "true");
            jdbcOptions.getUsername().ifPresent(user -> info.setProperty("user", user));
            jdbcOptions.getPassword().ifPresent(password -> info.setProperty("password", password));
            connection = driver.connect(jdbcOptions.getDbURL(), info);
            if (connection == null) {
                // Throw same exception as DriverManager.getConnection when no driver found to match
                // caller expectation.
                throw new SQLException("\033[31m[ConnectionProvider] - [GetOrEstablishConnection] - No suitable driver found for\033[0m" + jdbcOptions.getDbURL(), "08001");
            }
        }
        return connection;
    }

    public void closeConnection() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                LOG.warn("JDBC connection close failed.", e);
            } finally {
                //等待垃圾回收
                connection = null;
            }
        }
    }

    /*
    * 连接重置，先关闭，再重新获取
    * */
    public Connection reestablishConnection() throws SQLException, ClassNotFoundException {
        closeConnection();
        return getOrEstablishConnection();
    }
}
/*
*
* 入口核心逻辑
* */
class JdbcCore<In, Exe extends JdbcBatchExecutor<In> > extends RichOutputFormat<In> implements Flushable {


    public interface ByteHouseExecutorFactory<T extends JdbcBatchExecutor<?>>   extends SerializableFunction<RuntimeContext, T> {}

    private static final Logger LOG = LoggerFactory.getLogger(JdbcCore.class);
    protected final ConnectionProvider connectionProvider;
    private transient ScheduledExecutorService scheduler;
    private transient ScheduledFuture<?> scheduledFuture;
    private transient volatile Exception flushException;
    private final JdbcExecutionCoreOptions executionOptions;
    private final ByteHouseExecutorFactory<Exe> executorFactory;//执行器工厂,用于创建执行器
    private transient Exe executor;
    private transient volatile int batchCount = 0;
    private transient int totalCount = 0;
    private transient volatile boolean closed = false;
    private String threadName;
//    private transient Exe executor;
//    static{
//
//
//    }

    JdbcCore(
            @Nonnull ConnectionProvider connectionProvider,
            @Nonnull JdbcExecutionCoreOptions executionOptions,
            @Nonnull ByteHouseExecutorFactory<Exe> executorFactory ) {
        this.connectionProvider = Preconditions.checkNotNull(connectionProvider);
        this.executionOptions = Preconditions.checkNotNull(executionOptions);
        this.executorFactory = Preconditions.checkNotNull(executorFactory);

    }

    public String getThreadName() {
        if(threadName==null){
            threadName= "ByteHouseSink_"+Thread.currentThread().getId();
        }
        return threadName;


    }

    @Override
    public void configure(Configuration parameters) {}

    @Override
    public void open(InitializationContext context) throws IOException {
        try {
            connectionProvider.getOrEstablishConnection();
        } catch (Exception e) {
            throw new IOException("ThreadName="+getThreadName()+" - [JdbcCore] - [Open] - unable to open JDBC writer", e);
        }
        executor = createExecutor(executorFactory);
        if (executionOptions.getBatchIntervalMs() != 0 && executionOptions.getBatchSize() != 1) {
            this.scheduler =
                    Executors.newScheduledThreadPool(
                            1, new ExecutorThreadFactory("jdbc-upsert-output-format"));
            this.scheduledFuture =
                    this.scheduler.scheduleWithFixedDelay(() -> {
                        //因为线程需要一直运行，不能因为异常就停止，所以需要捕获异常
                        synchronized (JdbcCore.this) {
                            if (!closed) {
                                try {
                                    flush();
                                } catch (Exception e) {
                                    throw new RuntimeException("[JdbcCore] - [JdbcBatchExecutorScheduler] - Writing records to JDBC failed.",e);
                                }
                            }

                        }
                    },
                            executionOptions.getBatchIntervalMs(),
                            executionOptions.getBatchIntervalMs(),
                            TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public final synchronized void writeRecord(In record) throws IOException {
//        checkFlushException();
        try {
            executor.addToBuffer(record);
            batchCount++;
            totalCount++;
            if (executionOptions.getBatchSize() > 0 && batchCount >= executionOptions.getBatchSize()) {
                flush();
            }
        } catch (Exception e) {
            throw new IOException("\033[31mThreadName="+getThreadName()+" - [JdbcCore] - [WriteRecord] - Writing records to JDBC failed.\033[0m", e);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (!closed) {
            closed = true;

            if (this.scheduledFuture != null) {
                scheduledFuture.cancel(false);
                this.scheduler.shutdown();
            }

            if (batchCount > 0) {
                try {
                    flush();
                } catch (Exception e) {
                    throw new IOException("\033[31mThreadName="+getThreadName()+" - [JdbcCore] - [Close] - Writing records to JDBC failed.\033[0m", e);
                }
            }

            try {
                if (executor != null) {
                    executor.closeStatements();
                }
            } catch (SQLException e) {
                LOG.warn("\033[33mThreadName="+getThreadName()+" - [JdbcCore] - [Close] - Close JDBC writer failed.\033[0m", e);
            }
        }
        connectionProvider.closeConnection();
//        checkFlushException();
    }


    /*
    *
    * 执行器对象构造器
    * */
    private Exe createExecutor(ByteHouseExecutorFactory <Exe> executorFactory) throws IOException {
        Exe exec = executorFactory.apply(getRuntimeContext());
        try {
            exec.prepareStatements(connectionProvider.getConnection());
        } catch (SQLException e) {
            throw new IOException("\033[31mThreadName="+getThreadName()+" - [JdbcCore] - [createExecutor] - unable to open JDBC writer\033[0m", e);
        }
        return exec;
    }



    private void checkFlushException() {
        if (flushException != null) {
            throw new RuntimeException("\033[31mThreadName="+getThreadName()+" - [JdbcCore] - [CheckFlushException] - Writing records to JDBC failed.\033[0m", flushException);
        }
    }

    @Override
    public synchronized void flush() throws IOException {
//        checkFlushException();

        for (int i = 0; i <= executionOptions.getMaxRetries(); i++) {
            try {
                executor.executeBatch();
                batchCount = 0;
                LOG.info("\033[32mThreadName="+getThreadName()+" - [JdbcCore] - [Flush] - JDBC flush success, current totalCount = {}\033[0m", totalCount);
                break;
            } catch (SQLException e) {
                LOG.error("\033[31mThreadName="+getThreadName()+" - [JdbcCore] - [Flush] - executeBatch error, retry times = {}\033[0m",i);
                if (i >= executionOptions.getMaxRetries()) {
                    throw new IOException("\033[31mThreadName="+getThreadName()+" - [JdbcCore] - [Flush] - executeBatch error, The maximum number of retries is "+i+" which is exhausted\033[0m", e);
                }
                try {
                    if (!connectionProvider.isConnectionValid()) {
                        updateExecutor(true);
                        LOG.info("\033[32mThreadName="+getThreadName()+" - [JdbcCore] - [Flush] - connection Reestablish success.\033[0m");
                    }
                } catch (Exception exception) {
//                    LOG.error("[JdbcCore] - [Flush] - connection is not valid, and reestablish connection failed.", exception);
                    throw new IOException("\033[31mThreadName="+getThreadName()+" - [JdbcCore] - [Flush] - Reestablish JDBC connection failed\033[0m", exception);
                }
                try {
                    Thread.sleep(1000 * i);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IOException("\033[31mThreadName="+getThreadName()+" - [JdbcCore] - [Flush] - unable to flush; thread is interrupted while doing another attempt\033[0m", e);
                }
            }
        }

    }

    public void updateExecutor(boolean reconnect) throws SQLException, ClassNotFoundException {
        // 重置ps指针, 防止重连后重复录入数据, 这里非常重要
        executor.closeStatements();
        executor.prepareStatements(reconnect? connectionProvider.reestablishConnection(): connectionProvider.getConnection());
    }
}


public class JdbcBatchSink<T>  extends RichSinkFunction<T>  implements CheckpointedFunction{

    private final JdbcCore<T, ?> jdbcCore;

    private final Logger log = LoggerFactory.getLogger(JdbcBatchSink.class);
    private JdbcBatchSink( JdbcCore<T, ?> jdbcCore) {
        /*
        * 私有构造函数,确保对象的构造只通过下方的sink()函数完成，不可由别的地方随意创建对象
        * */
        this.jdbcCore = Preconditions.checkNotNull(jdbcCore);
    }

    public static <T> SinkFunction<T> sink( @Nonnull JdbcConnectionCoreOptions connectionOptions, @Nonnull JdbcExecutionCoreOptions executionOptions){
        /*
        * ConnectionProvider: 提供了数据库的连接管理，用于获取，释放连接
        * JdbcConnectionCoreOptions: 封装了连接参数
        * JdbcExecutionCoreOptions: 封装了执行参数，主要包含batchSize, batchIntervalMs, tries
        * */
        return new JdbcBatchSink<>(new JdbcCore<>(
                new ConnectionProvider(connectionOptions),
                executionOptions,
                runtimeContext -> JdbcBatchExecutor.simple(executionOptions.getTableName()) //这是一个匿名函数
            )
        );

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        RuntimeContext context = getRuntimeContext();
        jdbcCore.setRuntimeContext(context);
        int indexInSubtaskGroup = context.getIndexOfThisSubtask();
        int currentNumberOfSubtasks = context.getNumberOfParallelSubtasks();
        jdbcCore.open(
                new OutputFormat.InitializationContext() {
                    @Override
                    public int getNumTasks() {
                        return currentNumberOfSubtasks;
                    }

                    @Override
                    public int getTaskNumber() {
                        return indexInSubtaskGroup;
                    }

                    @Override
                    public int getAttemptNumber() {
                        return context.getAttemptNumber();
                    }
                });
        log.info("\033[32m[JdbcBatchSink] - jdbc connction build success \033[0m");

    }


    public void close() throws Exception {
        jdbcCore.close();
        log.info("\033[32m[JdbcBatchSink] - jdbc connction close success\033[0m");
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        /*
        * 不需要真正做快照, 简单写入下游即可
        * */
        jdbcCore.flush();
        log.info("[JdbcBatchSink] - snapshotState success.");

    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        /*
        *
        * 无需实现，因为恢复的时候直接从source直接重新拉取即可
        *
        * */

    }



    @Override
    public void invoke(T value, Context context) throws Exception {
        /*
         * 将数据发送到执行器缓存中, 由执行器统一写入到下游
         * */
        this.jdbcCore.writeRecord(value);
    }




}