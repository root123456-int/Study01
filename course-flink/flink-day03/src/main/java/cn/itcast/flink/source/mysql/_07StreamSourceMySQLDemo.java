package cn.itcast.flink.source.mysql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import scala.Int;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.TimeUnit;

/**
 * 从MySQL中实时加载数据：要求MySQL中的数据有变化，也能被实时加载出来
 */
public class _07StreamSourceMySQLDemo {

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 数据源-source
        DataStreamSource<Student> studentDStream = env.addSource(new MySQLSource());


        // 3. 数据转换-transformation
        // 4. 数据终端-sink
        studentDStream.printToErr();


        // 5. 触发执行-execute
        env.execute(_07StreamSourceMySQLDemo.class.getSimpleName());
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Student {
        private Integer id;
        private String name;
        private Integer age;
    }

    /**
     * 自定义数据源，从MySQL表中加载数据，并且实现增量加载
     */
    private static class MySQLSource extends RichParallelSourceFunction<Student> {
        private boolean isRunning = true;

        @Override
        public void run(SourceContext<Student> ctx) throws Exception {
            //TODO:定义一个增量加载标识符
            int countID = 0;

            while (true) {

                //a.加载驱动类
                Class.forName("com.mysql.jdbc.Driver");

                /*
                完整代码:从MySQL数据库加载表数据,JDBC方式
                 */
                Connection conn = null;
                PreparedStatement pstmt = null;
                ResultSet result = null;

                try {
                    //b. 获取连接
                    conn = DriverManager.getConnection(
                            "jdbc:mysql://192.168.88.101:3306/?useUnicode=true&characterEncoding=utf-8&useSSL=false",
                            "root",
                            "123456"
                    );

                    //c. 获取对象
                    pstmt = conn.prepareStatement("SELECT * FROM db_flink.t_student WHERE id > ?");

                    pstmt.setInt(1,countID);

                    //d. 查询数据
                    result = pstmt.executeQuery();

                    //e. 获取每条数据
                    while (result.next()) {
                        Integer id = result.getInt("id");
                        String name = result.getString(2);
                        Integer age = result.getInt("age");

                        //创建学生对象
                        Student student = new Student(id,name,age);

                        //更新标识ID
                        countID = id;
                        ctx.collect(student);
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if (null != result) result.close();
                    if (null != pstmt) pstmt.close();
                    if (null != conn) conn.close();
                }

                //休眠1s
                TimeUnit.SECONDS.sleep(1);

            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }


}

