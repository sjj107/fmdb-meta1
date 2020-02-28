package com.fiberhome.fmdb.pgtest;

import java.sql.*;

/**
 * @Description pg测试
 * @Author sjj
 * @Date 19/10/31 下午 04:06
 **/
public class PGConnTest {
    static String JDBC_URL = "jdbc:postgresql://172.17.30.6:5432/postgres";
    static String usr = "postgres";
    static String psd = "postgres";
    private static final String DRIVER_CLASS = "org.postgresql.Driver";

    public static void main(String[] args) {
        Connection connection = null;
        try {
            Class.forName(DRIVER_CLASS);
            connection = DriverManager.getConnection(JDBC_URL,usr,psd);
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("select * from tbl_type;");
            while (resultSet.next()) {
                String string = resultSet.getString(1);
                System.out.println(string);
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
