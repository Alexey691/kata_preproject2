package jm.task.core.jdbc.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Util {
    // реализуйте настройку соеденения с БД
    private static final String dbURL = "jdbc:mysql://localhost:3306/Test";
    private static final String username = "root";
    private static final String password = "root";

    public static Connection getConnection() {
        Connection connection;
        try {
            connection = DriverManager.getConnection(dbURL, username, password);
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return connection;
    }
}
