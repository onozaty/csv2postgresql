package com.enjoyxstudy.csv2postgresql;

import java.util.regex.Pattern;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.Value;

@Value
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class Column {

    /**
     * カラム名として使用しない文字のパターンです。
     */
    private static final Pattern UNUSABLE_CHARACTERS_IN_COLUMN_NAME = Pattern.compile("[^0-9a-zA-Z_]");

    private final String name;

    public static Column of(String baseName) {

        // 使用しない文字の場合には、アンダースコアに置換してカラム名に
        return new Column(
                UNUSABLE_CHARACTERS_IN_COLUMN_NAME.matcher(baseName).replaceAll("_").toLowerCase());
    }
}
