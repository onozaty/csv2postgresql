package com.enjoyxstudy.csv2postgresql;

import java.io.IOException;
import java.io.Reader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import lombok.Builder;
import lombok.Singular;
import lombok.Value;

@Value
public class Table {

    private final String name;

    private final List<Column> columns;

    private final String insertSql;

    @Builder
    public Table(String name, @Singular List<Column> columns) {
        this.name = name;
        this.columns = columns;

        // INSERTのSQLは繰り返し使うため、あらかじめ生成しておく
        this.insertSql = String.format(
                "INSERT INTO %s (%s) VALUES (%s)",
                name,
                columns.stream()
                        .map(Column::getName)
                        .collect(Collectors.joining(", ")),
                columns.stream()
                        .map(x -> "?")
                        .collect(Collectors.joining(", ")));
    }

    public boolean exists(Connection connection) throws SQLException {

        return new QueryRunner().query(
                connection,
                "SELECT EXISTS(SELECT 1 FROM pg_class WHERE relkind = 'r' AND relname = ?)",
                new ScalarHandler<>(),
                name);
    }

    public void create(Connection connection) throws SQLException {

        String columnDefinition = columns.stream()
                .map(column -> String.format("%s text", column.getName()))
                .collect(Collectors.joining(",\n"));

        String createTableSql = String.format(
                "CREATE TABLE %s (\n%s\n);",
                name,
                columnDefinition);

        new QueryRunner().update(
                connection,
                createTableSql);
    }

    public void insert(Connection connection, List<String[]> records) throws SQLException {

        new QueryRunner().batch(
                connection,
                insertSql,
                records.toArray(new Object[records.size()][]));
    }

    public long load(Connection connection, Reader csvReader) throws SQLException, IOException {

        String copySql = String.format(
                "COPY %s (%s) FROM STDIN (FORMAT csv, HEADER)",
                name,
                columns.stream()
                        .map(Column::getName)
                        .collect(Collectors.joining(", ")));
        
        return new CopyManager((BaseConnection)connection).copyIn(copySql, csvReader);
    }
}
