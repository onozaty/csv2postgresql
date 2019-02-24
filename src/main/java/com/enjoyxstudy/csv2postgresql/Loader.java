package com.enjoyxstudy.csv2postgresql;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class Loader {

    private final Config config;
    
    public static void main(String[] args) throws IOException, SQLException {

        if (args.length != 3) {
            System.err.println("usage: java -jar csv2postgresql-all.jar <config file> <csv file> <table name>");
            System.exit(1);
        }

        Config config = Config.of(Paths.get(args[0]));
        Path csvFilePath = Paths.get(args[1]);
        String tableName = args[2];

        System.out.println("Loading start...");

        int insertedCount = new Loader(config).load(csvFilePath, tableName);

        System.out.println(
                String.format("Loading is completed. %,d records were loaded.", insertedCount));
    }

    public int load(Path csvFilePath, String tableName)
            throws IOException, SQLException {

        try (
                Reader reader = Files.newBufferedReader(csvFilePath, Charset.forName(config.getCsvEncoding()));
                CSVParser parser = CSVFormat.EXCEL.withHeader().parse(reader)) {

            // ヘッダ名からカラムの情報を生成
            List<Column> columns = parser.getHeaderMap().entrySet().stream()
                    .sorted(Comparator.comparing(Entry::getValue)) // 記述順でソート
                    .map(Entry::getKey)
                    .map(Column::of)
                    .collect(Collectors.toList());

            Table table = Table.builder()
                    .name(tableName)
                    .columns(columns)
                    .build();

            try (Connection connection = DriverManager.getConnection(
                    config.getDatabaseUrl(),
                    config.getDatabaseUser(),
                    config.getDatabasePassword())) {

                connection.setAutoCommit(false);

                if (!table.exists(connection)) {
                    // テーブルが存在しなかった場合にはテーブル作成から
                    table.create(connection);
                }

                int insertedCount = 0;
                
                // 一定件数毎にINSERT
                List<String[]> insertTargetRecords = new ArrayList<>();
                for (CSVRecord record : parser) {
                    insertTargetRecords.add(toValues(record));

                    if (record.size() == config.getBatchInsertSize()) {
                        table.insert(connection, insertTargetRecords);
                        insertedCount += insertTargetRecords.size();
                        insertTargetRecords.clear();
                    }
                }

                if (!insertTargetRecords.isEmpty()) {
                    table.insert(connection, insertTargetRecords);
                    insertedCount += insertTargetRecords.size();
                }

                connection.commit();
                
                return insertedCount;
            }
        }
    }

    private String[] toValues(CSVRecord record) {

        List<String> values = new ArrayList<>();
        record.forEach(values::add);

        return values.toArray(new String[values.size()]);
    }
}
