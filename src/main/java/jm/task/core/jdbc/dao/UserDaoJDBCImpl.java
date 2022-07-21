package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class UserDaoJDBCImpl implements UserDao {

    Connection connection = Util.getConnection();
    private static final String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS Users (" +
            " id BIGINT AUTO_INCREMENT PRIMARY KEY" +
            ", name NVARCHAR(100)" +
            ", lastname NVARCHAR(100)" +
            ", age TINYINT);";
    private static final String DROP_TABLE = "DROP TABLE IF EXISTS Users;";
    private static final String INSERT_USER =
            "INSERT INTO Users (name, lastname, age) " +
                    "VALUES ( ?, ?, ? );";
    private static final String DELETE_USER = "DELETE FROM Users WHERE id = ?;";
    private static final String STRING_ALL_USERS = "SELECT name, lastname, age FROM Users ;";
    private static final String DELETE_ALL_USERS = "DELETE FROM Users;";

    public UserDaoJDBCImpl() {

    }

    public void createUsersTable() {

        try (Statement statement = connection.createStatement()) {
            statement.execute(CREATE_TABLE);
            connection.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void dropUsersTable() {
        try (Statement statement = connection.createStatement()) {
            statement.execute(DROP_TABLE);
            connection.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void saveUser(String name, String lastName, byte age) {
        try (PreparedStatement prepareStatement = connection.prepareStatement(INSERT_USER)) {
            prepareStatement.setString(1, name);
            prepareStatement.setString(2, lastName);
            prepareStatement.setByte(3, age);
            prepareStatement.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    public void removeUserById(long id) {
        try (PreparedStatement prepareStatement = connection.prepareStatement(DELETE_USER)) {
            prepareStatement.setLong(1, id);
            prepareStatement.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    public List<User> getAllUsers() {
        List<User> users = new ArrayList<>();
        try (PreparedStatement prepareStatement = connection.prepareStatement(STRING_ALL_USERS)) {
            ResultSet rs = prepareStatement.executeQuery();
            while (rs.next()) {
                users.add(new User(rs.getString("name"),
                        rs.getString("lastname"),
                        rs.getByte("age")));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return users;
    }

    public void cleanUsersTable() {
        try (Statement statement = connection.createStatement()) {
            statement.execute(DELETE_ALL_USERS);
            connection.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
