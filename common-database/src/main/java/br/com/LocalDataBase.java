package br.com;

import java.io.IOException;
import java.sql.*;

public class LocalDataBase {
    private final Connection connection;

    public LocalDataBase(String name) throws SQLException {
        String url = "jdbc:sqlite:target/" + name + ".db";
        this.connection = DriverManager.getConnection(url);
    }

    public void createIfNotExists(String sql) {
        try {
            connection.createStatement().execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void update(String s, String... params) throws SQLException {
        var statement = prepare(s, params);
        statement.execute();
    }

    private PreparedStatement prepare(String s, String[] params) throws SQLException {
        var statement = connection.prepareStatement(s);

        for (int i = 0; i < params.length; i++) {
            statement.setString(i + 1, params[i]);
        }
        return statement;
    }

    public ResultSet query(String s, String... params) throws SQLException {
        var statement = prepare(s, params);
        return statement.executeQuery();
    }

    public void close() throws IOException {
        try {
            connection.close();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }
}
