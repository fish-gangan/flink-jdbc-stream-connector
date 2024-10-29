package com.track;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Random;
//import com.mysql.cj.jdbc.Driver

//import com.mysql.jdbc.Driver.*


//        env.fromElements(
//                new Book(101L, "《乡野怪谈》",  "彭干"),
//                new Book(102L, "《麻衣神相》",  "彭干"),
//                new Book(103L, "《上海风云》",  "彭干"),
//                new Book(104L, "《大闹天宫》", "彭干")
//        ).addSink(
//                JdbcSink.sink(
//                        "insert into books (id, bookName, authors) values (?, ?, ?)",
//                        (statement, book) -> {
//                            statement.setLong(1, book.id);
//                            statement.setString(2, book.bookName);
//                            statement.setString(3, book.author);
//                        },
//                        JdbcExecutionOptions.builder()
//                                .withBatchSize(1000)
//                                .withBatchIntervalMs(200)
//                                .withMaxRetries(5)
//                                .build(),
//                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//                                .withUrl("jdbc:postgresql://dbhost:5432/postgresdb")
//                                .withDriverName("org.postgresql.Driver")
//                                .withUsername("someUser")
//                                .withPassword("somePassword")
//                                .build()
//                ));
//
//        env.execute();
@ToString
@Getter
@Setter
class Book{
    public Long id;
    public String log_date;
    public String bookName;
    public String author;
    public String address;
    public String app_01;
    public String app_02;
    public String app_03;
    public String app_04;
    public String app_05;
    public String app_06;
    public String app_07;
    public String app_08;
    public String app_09;
    public String app_10;
    public String app_11;
    public String app_12;
    public String app_13;
    public String app_14;
    public String app_15;
    public String app_16;
    public String app_17;
    public String app_18;
    public String app_19;
    public String app_20;
    public String app_21;
    public String app_22;
    public String app_23;
    public String app_24;
    public String app_25;
    public String app_26;
    public String app_27;
    public String app_28;
    public String app_29;
    public String app_30;
    public String app_31;


    public Book(){

    }
    public static Book getBean(){


        Book b= new Book();
        b.log_date="";
        b.log_date=generateDate();
        b.bookName="《权倾天下》";
        b.author="柯南";
        b.address="燕京";
        b.app_01="随机产生的字符串，用于测试！hello world.";
        b.app_02="随机产生的字符串，用于测试！hello world.";
        b.app_03="随机产生的字符串，用于测试！hello world.";
        b.app_05="随机产生的字符串，用于测试！hello world.";
        b.app_05="随机产生的字符串，用于测试！hello world.";
        b.app_06="随机产生的字符串，用于测试！hello world.";
        b.app_07="随机产生的字符串，用于测试！hello world.";
        b.app_08="随机产生的字符串，用于测试！hello world.";
        b.app_09="随机产生的字符串，用于测试！hello world.";
        b.app_10="随机产生的字符串，用于测试！hello world.";
        b.app_11="随机产生的字符串，用于测试！hello world.";
        b.app_12="随机产生的字符串，用于测试！hello world.";
        b.app_13="随机产生的字符串，用于测试！hello world.";
        b.app_14="随机产生的字符串，用于测试！hello world.";
        b.app_15="随机产生的字符串，用于测试！hello world.";
        b.app_16="随机产生的字符串，用于测试！hello world.";
        b.app_17="随机产生的字符串，用于测试！hello world.";
        b.app_18="随机产生的字符串，用于测试！hello world.";
        b.app_19="随机产生的字符串，用于测试！hello world.";
        b.app_20="随机产生的字符串，用于测试！hello world.";
        b.app_21="随机产生的字符串，用于测试！hello world.";
        b.app_22="随机产生的字符串，用于测试！hello world.";
        b.app_23="随机产生的字符串，用于测试！hello world.";
        b.app_24="随机产生的字符串，用于测试！hello world.";
        b.app_25="随机产生的字符串，用于测试！hello world.";
        b.app_26="随机产生的字符串，用于测试！hello world.";
        b.app_27="随机产生的字符串，用于测试！hello world.";
        b.app_28="随机产生的字符串，用于测试！hello world.";
        b.app_29="随机产生的字符串，用于测试！hello world.";
        b.app_30="随机产生的字符串，用于测试！hello world.";
        b.app_31="随机产生的字符串，用于测试！hello world.";
        return b;
    }

    public static String generateDate(){


                // 获取当前日期
                LocalDate currentDate = LocalDate.now();

                // 生成一个0到9之间的随机整数
                Random random = new Random();
                int randomDays = random.nextInt(10); // 包括0，不包括10，所以范围是0-9

                // 计算随机日期
                LocalDate randomDate = currentDate.plusDays(randomDays);

                // 格式化日期为yyyy-MM-dd字符串
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

        // 输出结果
                return randomDate.format(formatter);
    }





}

class MySource implements SourceFunction<Book> {

    private volatile boolean isRunning = true;
    @Override
    public void run(SourceContext<Book> ctx) throws Exception {
        long id= 1L;
        while (isRunning) {

           Book b=  Book.getBean();
           b.setId(id);
           ctx.collect(b);
           id=id+1;
        }
    }
    @Override
    public void cancel() {
        isRunning = false;

    }
}

