package com.enjoyxstudy.csv2postgresql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

/**
 * Tableのテストです。 
 * <p>Vagrantで事前にDBを起動しておく必要があります。</p>
 */
public class TableTest {

    private static final Config CONFIG = Config.builder()
            .databaseUrl("jdbc:postgresql://192.168.33.10:5432/testdb")
            .databaseUser("user1")
            .databasePassword("pass1")
            .build();

    @Test
    public void exists() throws SQLException {

        Table table = Table.builder()
                .name("test_table")
                .column(Column.of("col1"))
                .build();

        try (Connection connection = DriverManager.getConnection(
                CONFIG.getDatabaseUrl(),
                CONFIG.getDatabaseUser(),
                CONFIG.getDatabasePassword())) {

            // 前回のテストで存在したままの可能性があるので、いったん削除
            TestHelper.dropTableIfExists(connection, table.getName());

            assertThat(table.exists(connection)).isFalse();

            // テーブル作成
            table.create(connection);

            // 作成されていること
            assertThat(table.exists(connection)).isTrue();
        }
    }

    @Test
    public void insert_1カラム_1レコード() throws SQLException {

        Table table = Table.builder()
                .name("test_table")
                .column(Column.of("col1"))
                .build();

        try (Connection connection = DriverManager.getConnection(
                CONFIG.getDatabaseUrl(),
                CONFIG.getDatabaseUser(),
                CONFIG.getDatabasePassword())) {

            // 前回のテストで存在したままの可能性があるので、いったん削除
            TestHelper.dropTableIfExists(connection, table.getName());

            table.create(connection);

            List<String[]> records = new ArrayList<>();
            records.add(new String[] { "value" });
            table.insert(
                    connection,
                    records);

            // INSERTしたレコードを検索して確認
            assertThat(TestHelper.selectAll(connection, table.getName()))
                    .hasSize(1)
                    .extracting(x -> x.get("col1"))
                    .containsExactlyInAnyOrder("value");
        }
    }

    @Test
    public void insert_複数カラム_複数レコード() throws SQLException {

        Table table = Table.builder()
                .name("test_table")
                .column(Column.of("col1"))
                .column(Column.of("col2"))
                .column(Column.of("col3"))
                .build();

        try (Connection connection = TestHelper.getConnection(CONFIG)) {

            // 前回のテストで存在したままの可能性があるので、いったん削除
            TestHelper.dropTableIfExists(connection, table.getName());

            table.create(connection);

            List<String[]> records = Arrays.asList(
                    new String[] { "value1-1", "value1-2", "value1-3" },
                    new String[] { "value2-1", "value2-2", "value2-3" },
                    new String[] { "value3-1", "value3-2", "value3-3" },
                    new String[] { "value4-1", "value4-2", "value4-3" });
            table.insert(
                    connection,
                    records);

            // INSERTしたレコードを検索して確認
            assertThat(TestHelper.selectAll(connection, table.getName()))
                    .hasSize(4)
                    .extracting(x -> x.get("col1"), x -> x.get("col2"), x -> x.get("col3"))
                    .containsExactlyInAnyOrder(
                            tuple("value1-1", "value1-2", "value1-3"),
                            tuple("value2-1", "value2-2", "value2-3"),
                            tuple("value3-1", "value3-2", "value3-3"),
                            tuple("value4-1", "value4-2", "value4-3"));
        }
    }
}
