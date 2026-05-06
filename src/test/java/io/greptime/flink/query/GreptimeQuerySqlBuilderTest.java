package io.greptime.flink.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.util.List;
import org.junit.jupiter.api.Test;

class GreptimeQuerySqlBuilderTest {
    private final GreptimeQuerySqlBuilder builder = new GreptimeQuerySqlBuilder(GreptimeQueryDialect.MYSQL);

    @Test
    void buildsFullyQualifiedSelectWithQuotedIdentifiers() {
        String sql = builder.buildSelect("public", "metrics", List.of("host", "cpu"), 10L);

        assertEquals("SELECT `host`, `cpu` FROM `public`.`metrics` LIMIT 10", sql);
    }

    @Test
    void escapesBackticksInIdentifiers() {
        String sql = builder.buildSelect("tenant`a", "metrics`raw", List.of("host`name"), null);

        assertEquals("SELECT `host``name` FROM `tenant``a`.`metrics``raw`", sql);
    }

    @Test
    void buildsEmptyProjectionSelect() {
        String sql = builder.buildSelect("public", "metrics", List.of(), 0L);

        assertEquals("SELECT 1 FROM `public`.`metrics` LIMIT 0", sql);
    }
}
