package cn.itcast.flink.exactly.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

class DBConnectUtil {
    /**
     * 获取连接
     */
    public static Connection getConnection(String url, String user, String password) throws SQLException {
        Connection conn = null;
        conn = DriverManager.getConnection(url, user, password);
        //设置手动提交
        conn.setAutoCommit(false);
        return conn;
    }

    /**
     * 提交事物
     */
    public static void commit(Connection conn) {
        if (conn != null) {
            try {
                conn.commit();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                close(conn);
            }
        }
    }

    /**
     * 事物回滚
     */
    public static void rollback(Connection conn) {
        if (conn != null) {
            try {
                conn.rollback();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                close(conn);
            }
        }
    }

    /**
     * 关闭连接
     */
    public static void close(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}