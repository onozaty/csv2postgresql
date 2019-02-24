package com.enjoyxstudy.csv2postgresql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.sql.SQLException;

import org.junit.Test;

/**
 * Loaderのテストです。 
 * <p>Vagrantで事前にDBを起動しておく必要があります。</p>
 */
public class LoaderTest {

    @Test
    public void load_1カラム_1レコード() throws SQLException, IOException, URISyntaxException {

        String tableName = "test_table";
        Path configFilePath = TestHelper.getResourcePath("config.properties", this.getClass());
        Path csvFilePath = TestHelper.getResourcePath("1カラム1レコード.csv", this.getClass());

        Config config = Config.of(configFilePath);

        // 前回のテストで存在したままの可能性があるので、いったん削除
        TestHelper.dropTableIfExists(config, tableName);

        new Loader(config).load(csvFilePath, tableName);

        assertThat(TestHelper.selectAll(config, tableName))
                .hasSize(1)
                .extracting(x -> x.get("column1"))
                .containsExactlyInAnyOrder("value1");
    }

    @Test
    public void load_複数カラム_複数レコード() throws SQLException, IOException, URISyntaxException {

        String tableName = "test_table";
        Path configFilePath = TestHelper.getResourcePath("config.properties", this.getClass());
        Path csvFilePath = TestHelper.getResourcePath("複数カラム複数レコード.csv", this.getClass());

        Config config = Config.of(configFilePath);

        // 前回のテストで存在したままの可能性があるので、いったん削除
        TestHelper.dropTableIfExists(config, tableName);

        new Loader(config).load(csvFilePath, tableName);

        assertThat(TestHelper.selectAll(config, tableName))
                .hasSize(2)
                .extracting(x -> x.get("column1"), x -> x.get("column2"), x -> x.get("column3"))
                .containsExactlyInAnyOrder(
                        tuple("value1-1", "value1-2", "value1-3"),
                        tuple("value2-1", "value2-2", "value2-3"));
    }

    @Test
    public void load_同じテーブルに対して繰り返し実行() throws SQLException, IOException, URISyntaxException {

        String tableName = "test_table";
        Path configFilePath = TestHelper.getResourcePath("config.properties", this.getClass());
        Path csvFilePath = TestHelper.getResourcePath("複数カラム複数レコード.csv", this.getClass());

        Config config = Config.of(configFilePath);

        // 前回のテストで存在したままの可能性があるので、いったん削除
        TestHelper.dropTableIfExists(config, tableName);

        // 繰り返し登録
        Loader loader = new Loader(config);
        loader.load(csvFilePath, tableName);
        loader.load(csvFilePath, tableName);

        // 2レコード×2回で4レコード登録されていること
        assertThat(TestHelper.selectAll(config, tableName))
                .hasSize(4)
                .extracting(x -> x.get("column1"), x -> x.get("column2"), x -> x.get("column3"))
                .containsExactlyInAnyOrder(
                        tuple("value1-1", "value1-2", "value1-3"),
                        tuple("value2-1", "value2-2", "value2-3"),
                        tuple("value1-1", "value1-2", "value1-3"),
                        tuple("value2-1", "value2-2", "value2-3"));
    }
}
