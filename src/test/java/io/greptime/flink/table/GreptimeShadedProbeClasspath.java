package io.greptime.flink.table;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

final class GreptimeShadedProbeClasspath {
    private GreptimeShadedProbeClasspath() {}

    static String shadedRuntimeClasspath() throws IOException {
        Set<String> classpath = new LinkedHashSet<>();
        classpath.add(
                Paths.get("target/test-classes").toAbsolutePath().normalize().toString());
        classpath.add(locateShadedJar().toString());
        for (Path path : currentClasspathEntries()) {
            if (isFlinkRuntimeClasspathEntry(path)) {
                classpath.add(path.toAbsolutePath().normalize().toString());
            }
        }
        return String.join(File.pathSeparator, classpath);
    }

    private static Path locateShadedJar() throws IOException {
        Path target = Paths.get("target").toAbsolutePath().normalize();
        try (DirectoryStream<Path> jars = Files.newDirectoryStream(target, "*-shaded.jar")) {
            for (Path jar : jars) {
                return jar.toAbsolutePath().normalize();
            }
        }
        throw new IOException("Expected shaded jar under target/. Run mvn verify from the package lifecycle.");
    }

    private static List<Path> currentClasspathEntries() throws IOException {
        List<Path> entries = new ArrayList<>();
        String javaClasspath = System.getProperty("java.class.path", "");
        if (javaClasspath.isEmpty()) {
            return entries;
        }
        for (String entry : javaClasspath.split(File.pathSeparator)) {
            if (entry.isEmpty()) {
                continue;
            }
            Path path = Paths.get(entry).toAbsolutePath().normalize();
            entries.add(path);
            if (Files.isRegularFile(path) && path.toString().endsWith(".jar")) {
                entries.addAll(manifestClasspathEntries(path));
            }
        }
        return entries;
    }

    private static List<Path> manifestClasspathEntries(Path jarPath) throws IOException {
        try (JarFile jarFile = new JarFile(jarPath.toFile())) {
            Manifest manifest = jarFile.getManifest();
            if (manifest == null) {
                return List.of();
            }
            String classpath = manifest.getMainAttributes().getValue(Attributes.Name.CLASS_PATH);
            if (classpath == null || classpath.isBlank()) {
                return List.of();
            }

            List<Path> entries = new ArrayList<>();
            for (String entry : classpath.split(" ")) {
                if (entry.isBlank()) {
                    continue;
                }
                URI uri = URI.create(entry);
                Path path = uri.isAbsolute()
                        ? Paths.get(uri)
                        : jarPath.getParent().resolve(entry).normalize();
                entries.add(path.toAbsolutePath().normalize());
            }
            return entries;
        }
    }

    private static boolean isFlinkRuntimeClasspathEntry(Path path) {
        String value = path.toString();
        if (value.endsWith("target/classes")
                || value.contains("greptimedb-flink-connector-1.20")
                || value.contains("/io/greptime/")
                || value.contains("/io/grpc/")
                || value.contains("/io/netty/")
                || value.contains("/org/apache/arrow/")
                || value.contains("/com/fasterxml/")
                || value.contains("/com/google/api/")
                || value.contains("/com/google/android/")
                || value.contains("/com/google/code/gson/")
                || value.contains("/com/google/flatbuffers/")
                || value.contains("/com/google/guava/")
                || value.contains("/com/google/protobuf/")
                || value.contains("/com/netflix/")
                || value.contains("/com/github/luben/")
                || value.contains("/io/dropwizard/")
                || value.contains("/javax/annotation/")
                || value.contains("/org/checkerframework/")
                || value.contains("/org/codehaus/mojo/")
                || value.contains("/org/junit/")
                || value.contains("/junit/")
                || value.contains("/org/testcontainers/")
                || value.contains("/com/mysql/")) {
            return false;
        }

        return value.contains("/org/apache/flink/")
                || value.contains("/org/apache/commons/")
                || value.contains("/commons-")
                || value.contains("/com/esotericsoftware/")
                || value.contains("/com/twitter/")
                || value.contains("/org/objenesis/")
                || value.contains("/com/ibm/icu/")
                || value.contains("/org/snakeyaml/")
                || value.contains("/org/xerial/snappy/")
                || value.contains("/org/lz4/")
                || value.contains("/org/javassist/")
                || value.contains("/org/slf4j/")
                || value.contains("/com/google/code/findbugs/");
    }
}
