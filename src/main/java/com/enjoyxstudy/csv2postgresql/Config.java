package com.enjoyxstudy.csv2postgresql;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class Config {

    private final String databaseUrl;

    private final String databaseUser;

    private final String databasePassword;

    private final int batchInsertSize;

    private final String csvEncoding;

    public static Config of(Path configFilePath) throws IOException {

        Properties properties = new Properties();
        try (InputStream inputStream = Files.newInputStream(configFilePath)) {

            properties.load(inputStream);

            return Config.builder()
                    .databaseUrl(properties.getProperty("database.url"))
                    .databaseUser(properties.getProperty("database.user"))
                    .databasePassword(properties.getProperty("database.password"))
                    // デフォルトは1000件ずつ
                    .batchInsertSize(Integer.parseInt(properties.getProperty("database.batch-insert-size", "1000")))
                    .csvEncoding(properties.getProperty("csv.encoding"))
                    .build();
        }
    }
}
