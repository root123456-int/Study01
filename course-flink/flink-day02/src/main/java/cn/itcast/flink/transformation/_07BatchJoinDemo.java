package cn.itcast.flink.transformation;

import lombok.Data;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.CrossOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.operators.join.JoinFunctionAssigner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

/**
 * Flink 框架中批处理实现两个数据集关联分析：JOIN
 */
public class _07BatchJoinDemo {
    @Data
    public static class Score {
        private Integer id;
        private String stuName;
        private Integer subId;
        private Double score;
    }

    @Data
    public static class Subject {
        private Integer id;
        private String name;
    }

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 数据源-source
        // 学生成绩数据score.csv: datas/score.csv
        DataSource<Score> scoreDataSource = env.readCsvFile("datas/score.csv")
                .fieldDelimiter(",")
                .pojoType(Score.class, "id", "stuName", "subId", "score");

        // 学科数据subject.csv: datas/subject.csv
        DataSource<Subject> subjectDataSource = env.readCsvFile("datas/subject.csv")
                .fieldDelimiter(",")
                .pojoType(Subject.class, "id", "name");

        // 3. 数据转换-transformation
        // TODO: 等值JOIN
        // SQL: SELECT ... FROM score sc JOIN subject sb ON sc.subId = sb.id ;
        JoinOperator.EquiJoin<Score, Subject, String> joinDataSet = scoreDataSource
                .join(subjectDataSource)
                .where("subId").equalTo("id")
                .with(new JoinFunction<Score, Subject, String>() {
                    @Override
                    public String join(Score score, Subject subject) throws Exception {
                        return score.id + ", " + score.stuName + ", " + subject.name + ", " + score.score;
                    }
                });

        //joinDataSet.printToErr();


        // TODO： 外连接函数使用
        DataSource<Tuple2<Integer, String>> userDataSet = env.fromElements(
                Tuple2.of(1, "tom"), Tuple2.of(2, "jack"), Tuple2.of(3, "rose")
        );
        DataSource<Tuple2<Integer, String>> cityDataSet = env.fromElements(
                Tuple2.of(1, "北京"), Tuple2.of(2, "上海"), Tuple2.of(4, "广州")
        );
        // TODO: 左外连接leftJoin

        JoinOperator<Tuple2<Integer, String>, Tuple2<Integer, String>, String> leftJoinDataSet = userDataSet
                .leftOuterJoin(cityDataSet)
                .where(0).equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, String>() {
                    @Override
                    public String join(Tuple2<Integer, String> user, Tuple2<Integer, String> city) throws Exception {

                        if (null == city) {
                            return user.f0 + ", " + user.f1 + ", " + "未知";
                        } else {

                        }
                        return user.f0 + ", " + user.f1 + ", " + city.f1;
                    }
                });
        leftJoinDataSet.printToErr();

        System.out.println("==================");

        // TODO: 右外连接rightJoin

        JoinOperator<Tuple2<Integer, String>, Tuple2<Integer, String>, String> rightJoinDataSet = userDataSet
                .rightOuterJoin(cityDataSet)
                .where(0).equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, String>() {
                    @Override
                    public String join(Tuple2<Integer, String> user, Tuple2<Integer, String> city) throws Exception {
                        if (null == user) {
                            return city.f0 + ", " + "未知" + ", " + city.f1;
                        } else {
                            return city.f0 + ", " + user.f1 + ", " + city.f1;
                        }

                    }
                });

        rightJoinDataSet.print();
        System.out.println("=========================");


        // TODO: 全外连接

        JoinOperator<Tuple2<Integer, String>, Tuple2<Integer, String>, String> fullJoinDataSet = userDataSet
                .fullOuterJoin(cityDataSet)
                .where(0).equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, String>() {
                    @Override
                    public String join(Tuple2<Integer, String> user, Tuple2<Integer, String> city) throws Exception {
                        if (null == user) {
                            return city.f0 + ", " + "未知" + ", " + city.f1;
                        } else if (null == city) {
                            return user.f0 + ", " + user.f1 + ", " + "未知";
                        } else {
                            return user.f0 + ", " + user.f1 + ", " + city.f1;
                        }
                    }
                });
        fullJoinDataSet.printToErr();

        System.out.println("=====================");


        // TODO: cross，笛卡尔积

        CrossOperator.DefaultCross<Tuple2<Integer, String>, Tuple2<Integer, String>> crossDataSet = userDataSet.cross(cityDataSet);
        crossDataSet.print();
    }

}
