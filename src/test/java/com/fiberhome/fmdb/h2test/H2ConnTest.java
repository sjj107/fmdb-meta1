package com.fiberhome.fmdb.h2test;

import java.sql.*;

/**
 * Created by sjj on 19/10/21
 * 测试连接H2
 */
public class H2ConnTest {
    /**
     * 使用TCP/IP的服务器模式（远程连接）方式连接H2数据库
     */
    private static final String JDBC_URL = "jdbc:h2:tcp://172.17.30.6:7770/data/H2/DB";

    private static final String USER = "root";
    private static final String PASSWORD = "123456";
    private static final String DRIVER_CLASS = "org.h2.Driver";

    public static void main(String[] args) throws Exception {
        transaction();
    }

    public static void transaction() {
        Connection connection = null;
        try {
            Class.forName(DRIVER_CLASS);
            String dbssql = "INSERT INTO DBS(NAME) VALUES (?);";
            String tbl_type = "INSERT INTO TBL_TYPE(TYPE_NAME) VALUES (?);";
            connection = DriverManager.getConnection(JDBC_URL);
            connection.setAutoCommit(false);
            PreparedStatement preparedStatement = connection.prepareStatement(dbssql);
            preparedStatement.setString(1, "dbname");
            preparedStatement.addBatch();
            preparedStatement.setString(1, "dbname3");
            preparedStatement.addBatch();
            preparedStatement.executeBatch();
            preparedStatement = connection.prepareStatement(tbl_type);
            preparedStatement.setString(1, "tbl_type");
            preparedStatement.addBatch();
            preparedStatement.executeBatch();
            connection.commit();
        } catch (Exception e) {
            e.printStackTrace();
            try {
                connection.rollback();
                System.out.println("回滚成功");
            } catch (SQLException ex) {
                System.out.println("回滚失败");
                ex.printStackTrace();
            }
        }


    }

    public static void simpleTest() throws Exception {
        Class.forName(DRIVER_CLASS);
        Connection connection = DriverManager.getConnection(JDBC_URL);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("show tables;");
        while (resultSet.next()) {
            String string = resultSet.getString(1);
            System.out.println(string);
        }
//        System.out.println(resultSet);
    }
}
