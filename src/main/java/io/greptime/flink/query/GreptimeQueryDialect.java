package io.greptime.flink.query;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

public enum GreptimeQueryDialect {
    MYSQL("jdbc:mysql:", "mysql-jdbc", "`", "connectTimeout", "socketTimeout", "com.mysql.cj.jdbc.Driver");

    private static final List<String> SENSITIVE_QUERY_KEY_FRAGMENTS =
            List.of("password", "passwd", "pwd", "token", "secret", "apikey", "auth", "credential");

    private final String jdbcUrlPrefix;
    private final String protocolName;
    private final String identifierQuote;
    private final String connectTimeoutProperty;
    private final String socketTimeoutProperty;
    private final String driverClassName;

    GreptimeQueryDialect(
            String jdbcUrlPrefix,
            String protocolName,
            String identifierQuote,
            String connectTimeoutProperty,
            String socketTimeoutProperty,
            String driverClassName) {
        this.jdbcUrlPrefix = jdbcUrlPrefix;
        this.protocolName = protocolName;
        this.identifierQuote = identifierQuote;
        this.connectTimeoutProperty = connectTimeoutProperty;
        this.socketTimeoutProperty = socketTimeoutProperty;
        this.driverClassName = driverClassName;
    }

    public static GreptimeQueryDialect fromJdbcUrl(String jdbcUrl) {
        Objects.requireNonNull(jdbcUrl, "jdbcUrl");
        String normalizedUrl = jdbcUrl.toLowerCase(Locale.ROOT);
        if (normalizedUrl.startsWith(MYSQL.jdbcUrlPrefix)) {
            return MYSQL;
        }
        throw new IllegalArgumentException(
                "GreptimeDB query source currently supports only MySQL JDBC (`jdbc:mysql:`), but `query.jdbc-url` was: "
                        + MYSQL.redactJdbcUrl(jdbcUrl));
    }

    public String protocolName() {
        return protocolName;
    }

    public String connectTimeoutProperty() {
        return connectTimeoutProperty;
    }

    public String socketTimeoutProperty() {
        return socketTimeoutProperty;
    }

    public void ensureJdbcDriverAvailable() {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = GreptimeQueryDialect.class.getClassLoader();
        }
        try {
            Class.forName(driverClassName, true, classLoader);
        } catch (ClassNotFoundException ignored) {
            // The JDBC driver may already be registered through another classloader.
        }
    }

    public Set<String> timeoutQueryKeys() {
        return Set.of(connectTimeoutProperty, socketTimeoutProperty);
    }

    public String quoteIdentifier(String identifier) {
        Objects.requireNonNull(identifier, "identifier");
        return identifierQuote
                + identifier.replace(identifierQuote, identifierQuote + identifierQuote)
                + identifierQuote;
    }

    public Set<String> queryParameterKeys(String jdbcUrl) {
        Set<String> keys = new LinkedHashSet<>();
        for (String normalizedKey : queryParameters(jdbcUrl, true)) {
            if (!normalizedKey.isEmpty()) {
                keys.add(normalizedKey);
            }
        }
        return keys;
    }

    public boolean hasSensitiveQueryKey(String jdbcUrl) {
        try {
            for (String normalizedKey : queryParameters(jdbcUrl, false)) {
                if (isSensitiveQueryKey(normalizedKey)) {
                    return true;
                }
            }
            return false;
        } catch (IllegalArgumentException e) {
            return true;
        }
    }

    public boolean hasSensitiveMaterial(String jdbcUrl) {
        return hasAuthorityUserInfo(jdbcUrl) || hasSensitiveQueryKey(jdbcUrl);
    }

    boolean hasAuthorityUserInfo(String jdbcUrl) {
        Objects.requireNonNull(jdbcUrl, "jdbcUrl");
        return authorityUserInfoEnd(jdbcUrl) >= 0;
    }

    public String redactJdbcUrl(String jdbcUrl) {
        Objects.requireNonNull(jdbcUrl, "jdbcUrl");
        String redactedUrl = redactAuthorityUserInfo(jdbcUrl);
        int queryStart = redactedUrl.indexOf('?');
        if (queryStart < 0 || queryStart == redactedUrl.length() - 1) {
            return redactedUrl;
        }

        int fragmentStart = redactedUrl.indexOf('#', queryStart + 1);
        String prefix = redactedUrl.substring(0, queryStart + 1);
        String query = fragmentStart < 0
                ? redactedUrl.substring(queryStart + 1)
                : redactedUrl.substring(queryStart + 1, fragmentStart);
        String fragment = fragmentStart < 0 ? "" : redactedUrl.substring(fragmentStart);

        StringBuilder redacted = new StringBuilder(prefix.length() + query.length());
        redacted.append(prefix);
        String[] parameters = query.split("&", -1);
        for (int i = 0; i < parameters.length; i++) {
            if (i > 0) {
                redacted.append('&');
            }
            String parameter = parameters[i];
            int valueStart = parameter.indexOf('=');
            String key = valueStart < 0 ? parameter : parameter.substring(0, valueStart);
            String normalizedKey;
            try {
                normalizedKey = parseQueryParameter(parameter, false);
            } catch (IllegalArgumentException e) {
                redacted.append("****");
                continue;
            }
            if (!normalizedKey.isEmpty() && isSensitiveQueryKey(normalizedKey)) {
                redacted.append(key).append("=****");
            } else {
                redacted.append(parameter);
            }
        }
        redacted.append(fragment);
        return redacted.toString();
    }

    private static List<String> queryParameters(String jdbcUrl, boolean validateValues) {
        int queryStart = jdbcUrl.indexOf('?');
        if (queryStart < 0 || queryStart == jdbcUrl.length() - 1) {
            return List.of();
        }

        int fragmentStart = jdbcUrl.indexOf('#', queryStart + 1);
        String query = fragmentStart < 0
                ? jdbcUrl.substring(queryStart + 1)
                : jdbcUrl.substring(queryStart + 1, fragmentStart);
        if (query.isEmpty()) {
            return List.of();
        }

        List<String> parameters = new ArrayList<>();
        for (String parameter : query.split("&", -1)) {
            if (!parameter.isEmpty()) {
                parameters.add(parseQueryParameter(parameter, validateValues));
            }
        }
        return parameters;
    }

    private static String parseQueryParameter(String parameter, boolean validateValue) {
        int valueStart = parameter.indexOf('=');
        String rawKey = valueStart < 0 ? parameter : parameter.substring(0, valueStart);
        String decodedKey = decodeQueryComponent(rawKey);
        if (validateValue && valueStart >= 0) {
            decodeQueryComponent(parameter.substring(valueStart + 1));
        }
        return decodedKey.toLowerCase(Locale.ROOT);
    }

    private static boolean isSensitiveQueryKey(String normalizedKey) {
        if ("user".equals(normalizedKey) || "username".equals(normalizedKey)) {
            return true;
        }
        String compactKey = normalizedKey.replace("_", "").replace("-", "").replace(".", "");
        for (String fragment : SENSITIVE_QUERY_KEY_FRAGMENTS) {
            if (normalizedKey.contains(fragment) || compactKey.contains(fragment)) {
                return true;
            }
        }
        return false;
    }

    private static String decodeQueryComponent(String value) {
        try {
            return URLDecoder.decode(value, StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid percent-encoding in `query.jdbc-url`", e);
        }
    }

    private static String redactAuthorityUserInfo(String jdbcUrl) {
        int userInfoEnd = authorityUserInfoEnd(jdbcUrl);
        if (userInfoEnd < 0) {
            return jdbcUrl;
        }
        int authorityStart = jdbcUrl.indexOf("://") + 3;
        return jdbcUrl.substring(0, authorityStart) + "****" + jdbcUrl.substring(userInfoEnd);
    }

    private static int authorityUserInfoEnd(String jdbcUrl) {
        int authorityStart = jdbcUrl.indexOf("://");
        if (authorityStart < 0) {
            return -1;
        }
        authorityStart += 3;
        int authorityEnd = authorityEnd(jdbcUrl, authorityStart);
        int userInfoEnd = jdbcUrl.lastIndexOf('@', authorityEnd - 1);
        return userInfoEnd >= authorityStart ? userInfoEnd : -1;
    }

    private static int authorityEnd(String jdbcUrl, int authorityStart) {
        int authorityEnd = jdbcUrl.length();
        for (char delimiter : new char[] {'/', '?', '#'}) {
            int delimiterIndex = jdbcUrl.indexOf(delimiter, authorityStart);
            if (delimiterIndex >= 0 && delimiterIndex < authorityEnd) {
                authorityEnd = delimiterIndex;
            }
        }
        return authorityEnd;
    }
}
