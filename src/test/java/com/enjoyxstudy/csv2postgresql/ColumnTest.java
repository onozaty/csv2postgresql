package com.enjoyxstudy.csv2postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class ColumnTest {

    @Test
    public void fromHeaderName_許可する文字のみ() {

        Column column = Column.of("azAZ0123456789_");
        assertThat(column)
                .returns("azaz0123456789_", Column::getName);
    }

    @Test
    public void fromHeaderName_許可しない文字含む() {

        Column column = Column.of("AZ -あ");
        assertThat(column)
                .returns("az___", Column::getName);
    }
}
