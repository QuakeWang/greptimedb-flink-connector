package io.greptime.flink.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class GreptimeShadedSourceDriverMissingIT {
    @Test
    void shadedJarSourceDiscoversFactoryAndReportsMissingMysqlDriverWithRedactedJdbcUrl() throws Exception {
        ProcessResult result = runProbe();

        assertEquals(0, result.exitCode, result.output);
        assertTrue(result.output.contains("MySQL-compatible JDBC driver must be available on the Flink classpath"));
        assertTrue(result.output.contains("jdbc:mysql://127.0.0.1:4002/public?useSSL=false"));
        assertFalse(result.output.contains("source-secret"));
        assertFalse(result.output.contains("source-token"));
        assertFalse(result.output.contains("source_user"));
    }

    private ProcessResult runProbe() throws Exception {
        Path outputFile = Files.createTempFile("greptime-shaded-source-driver-missing-probe-", ".log");
        ProcessBuilder processBuilder = new ProcessBuilder(
                javaExecutable(),
                "-cp",
                GreptimeShadedProbeClasspath.shadedRuntimeClasspath(),
                GreptimeShadedSourceDriverMissingProbe.class.getName());
        processBuilder.redirectErrorStream(true);
        processBuilder.redirectOutput(outputFile.toFile());

        Process process = processBuilder.start();
        try {
            boolean finished = process.waitFor(60, TimeUnit.SECONDS);
            String output = Files.readString(outputFile, StandardCharsets.UTF_8);
            if (!finished) {
                process.destroyForcibly();
                return new ProcessResult(-1, "Timed out waiting for shaded source driver probe\n" + output);
            }
            return new ProcessResult(process.exitValue(), output);
        } finally {
            Files.deleteIfExists(outputFile);
        }
    }

    private String javaExecutable() {
        return Paths.get(System.getProperty("java.home"), "bin", "java").toString();
    }

    private static final class ProcessResult {
        private final int exitCode;
        private final String output;

        private ProcessResult(int exitCode, String output) {
            this.exitCode = exitCode;
            this.output = output;
        }
    }
}
