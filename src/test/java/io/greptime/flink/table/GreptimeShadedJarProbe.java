package io.greptime.flink.table;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

final class GreptimeShadedJarProbe {
    private GreptimeShadedJarProbe() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            throw new IllegalArgumentException("Expected arguments: host port table");
        }

        String host = args[0];
        String port = args[1];
        String table = args[2];
        TableEnvironment tableEnv = TableEnvironment.create(
                EnvironmentSettings.newInstance().inBatchMode().build());

        tableEnv.executeSql(String.format(
                        "CREATE TEMPORARY TABLE shaded_smoke_sink ("
                                + " host STRING,"
                                + " cpu DOUBLE,"
                                + " ts TIMESTAMP(3) NOT NULL"
                                + ") WITH ("
                                + " 'connector' = 'greptimedb',"
                                + " 'endpoints' = '%s:%s',"
                                + " 'database' = 'public',"
                                + " 'table' = '%s',"
                                + " 'time-index' = 'ts',"
                                + " 'tags' = 'host',"
                                + " 'batch.max-rows' = '1'"
                                + ")",
                        host, port, table))
                .await();

        tableEnv.executeSql("INSERT INTO shaded_smoke_sink VALUES "
                        + "('host-shaded', 0.42, TIMESTAMP '2026-04-28 15:00:00.000')")
                .await();
    }
}
