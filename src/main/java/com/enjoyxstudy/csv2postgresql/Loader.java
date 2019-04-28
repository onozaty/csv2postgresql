package com.enjoyxstudy.csv2postgresql;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
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
import java.util.function.LongConsumer;
import java.util.stream.Collectors;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.input.BOMInputStream;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class Loader {

    private final Config config;

    public static void main(String[] args) throws IOException, SQLException {

        if (args.length != 3) {
            System.err.println("usage: java -jar csv2postgresql-all.jar <config file> <table name> <csv file>");
            System.exit(1);
        }

        Config config = Config.of(Paths.get(args[0]));
        String tableName = args[1];
        Path csvFilePath = Paths.get(args[2]);

        long startTime = System.currentTimeMillis();

        System.out.println("\nLoading...");

        long loadedCount = new Loader(config).loadByCopy(
                csvFilePath,
                tableName);

        System.out.println(
                String.format(
                        "Loading is completed. (Number of records: %,d / Elapsed millsecods: %,d)",
                        loadedCount,
                        System.currentTimeMillis() - startTime));
    }

    public long loadByCopy(Path csvFilePath, String tableName)
            throws IOException, SQLException {

        List<Column> columns;
        try (CSVParser csvParser = CSVFormat.EXCEL.withHeader().parse(newCsvReader(csvFilePath))) {

            columns = readColumns(csvParser);
        }

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

            long insertedCount;
            try (Reader csvReader = newCsvReader(csvFilePath)) {
                insertedCount = table.load(connection, csvReader);
            }

            connection.commit();

            return insertedCount;
        }
    }

    public long load(Path csvFilePath, String tableName, LongConsumer loadingNotifier)
            throws IOException, SQLException {

        try (CSVParser csvParser = CSVFormat.EXCEL.withHeader().parse(newCsvReader(csvFilePath))) {

            List<Column> columns = readColumns(csvParser);

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

                long insertedCount = 0;

                // 一定件数毎にINSERT
                List<String[]> insertTargetRecords = new ArrayList<>();
                for (CSVRecord record : csvParser) {
                    insertTargetRecords.add(toValues(record));

                    if (insertTargetRecords.size() == config.getBatchInsertSize()) {
                        table.insert(connection, insertTargetRecords);
                        insertedCount += insertTargetRecords.size();
                        loadingNotifier.accept(insertedCount);
                        insertTargetRecords.clear();
                    }
                }

                if (!insertTargetRecords.isEmpty()) {
                    table.insert(connection, insertTargetRecords);
                    insertedCount += insertTargetRecords.size();
                    loadingNotifier.accept(insertedCount);
                }

                connection.commit();

                return insertedCount;
            }
        }
    }

    public long load(Path csvFilePath, String tableName)
            throws IOException, SQLException {
        return load(csvFilePath, tableName, x -> {
        });
    }

    private InputStreamReader newCsvReader(Path csvFilePath) throws IOException {

        return new InputStreamReader(
                new BufferedInputStream(
                        // UTF-8のBOMを考慮
                        new BOMInputStream(Files.newInputStream(csvFilePath))),
                Charset.forName(config.getCsvEncoding()));
    }

    private List<Column> readColumns(CSVParser csvParser) {

        // ヘッダ名からカラムの情報を生成
        return csvParser.getHeaderMap().entrySet().stream()
                .sorted(Comparator.comparing(Entry::getValue)) // 記述順でソート
                .map(Entry::getKey)
                .map(Column::of)
                .collect(Collectors.toList());
    }

    private String[] toValues(CSVRecord record) {

        List<String> values = new ArrayList<>();
        record.forEach(values::add);

        return values.toArray(new String[values.size()]);
    }
}
