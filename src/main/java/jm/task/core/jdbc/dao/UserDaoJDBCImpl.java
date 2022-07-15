package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class UserDaoJDBCImpl implements UserDao {

    public UserDaoJDBCImpl() {

    }

    public void createUsersTable() {
        String query = "CREATE TABLE IF NOT EXISTS Users (" +
                " id BIGINT AUTO_INCREMENT PRIMARY KEY" +
                ", name NVARCHAR(100)" +
                ", lastname NVARCHAR(100)" +
                ", age TINYINT);";

        try (Connection conn = Util.getConnection();
             Statement statement = conn.createStatement()) {
            statement.execute(query);
            conn.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void dropUsersTable() {
        String query = "DROP TABLE IF EXISTS Users;";

        try (Connection conn = Util.getConnection();
             Statement statement = conn.createStatement()) {
            statement.execute(query);
            conn.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void saveUser(String name, String lastName, byte age) {
        String query = "INSERT INTO Users (name, lastname, age) VALUES ( ?, ?, ? );";

        try (Connection conn = Util.getConnection();
             PreparedStatement prepareStatement = conn.prepareStatement(query)) {
            prepareStatement.setString(1, name);
            prepareStatement.setString(2, lastName);
            prepareStatement.setByte(3, age);
            prepareStatement.executeUpdate();
            conn.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    public void removeUserById(long id) {

        String query = "DELETE FROM Users WHERE id = ?;";

        try (Connection conn = Util.getConnection();
             PreparedStatement prepareStatement = conn.prepareStatement(query)) {
            prepareStatement.setLong(1, id);
            prepareStatement.executeUpdate();
            conn.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    public List<User> getAllUsers() {
        List<User> users = new ArrayList<>();
        String query = "SELECT name, lastname, age FROM Users ;";
        try (Connection conn = Util.getConnection();
             PreparedStatement prepareStatement = conn.prepareStatement(query)) {

            ResultSet rs = prepareStatement.executeQuery();
            while (rs.next()) {
                String name = rs.getString("name");
                String lastname = rs.getString("lastname");
                byte age = rs.getByte("age");
                users.add(new User(name, lastname, age));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return users;
    }

    public void cleanUsersTable() {
        String query = "DELETE FROM Users;";

        try (Connection conn = Util.getConnection();
             Statement statement = conn.createStatement()) {
            statement.execute(query);
            conn.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
