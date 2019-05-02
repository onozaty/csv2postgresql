package com.enjoyxstudy.csv2postgresql;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapListHandler;

public class TestHelper {

    public static Connection getConnection(Config config) throws SQLException {

        return DriverManager.getConnection(
                config.getDatabaseUrl(),
                config.getDatabaseUser(),
                config.getDatabasePassword());
    }

    public static void dropTableIfExists(Config config, String tableName) throws SQLException {

        try (Connection connection = TestHelper.getConnection(config)) {
            dropTableIfExists(connection, tableName);
        }
    }

    public static void dropTableIfExists(Connection connection, String tableName) throws SQLException {

        new QueryRunner().update(
                connection,
                "DROP TABLE IF EXISTS " + tableName);
    }

    public static List<Map<String, Object>> selectAll(Config config, String tableName) throws SQLException {

        try (Connection connection = TestHelper.getConnection(config)) {
            return selectAll(connection, tableName);
        }
    }

    public static List<Map<String, Object>> selectAll(Connection connection, String tableName) throws SQLException {

        return new QueryRunner().query(
                connection,
                "SELECT * FROM " + tableName,
                new MapListHandler());
    }

    public static Path getResourcePath(String fileName, Class<?> clazz) throws URISyntaxException {
        return Paths.get(clazz.getResource(fileName).toURI());
    }
}
