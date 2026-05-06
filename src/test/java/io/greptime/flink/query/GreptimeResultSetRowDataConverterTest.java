package io.greptime.flink.query;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class GreptimeResultSetRowDataConverterTest {
    @Test
    void convertsSupportedJdbcValuesToRowData() throws Exception {
        RowType rowType = (RowType) DataTypes.ROW(
                        DataTypes.FIELD("flag", DataTypes.BOOLEAN()),
                        DataTypes.FIELD("tiny_value", DataTypes.TINYINT()),
                        DataTypes.FIELD("small_value", DataTypes.SMALLINT()),
                        DataTypes.FIELD("int_value", DataTypes.INT()),
                        DataTypes.FIELD("big_value", DataTypes.BIGINT()),
                        DataTypes.FIELD("float_value", DataTypes.FLOAT()),
                        DataTypes.FIELD("double_value", DataTypes.DOUBLE()),
                        DataTypes.FIELD("text_value", DataTypes.STRING()),
                        DataTypes.FIELD("binary_value", DataTypes.BYTES()),
                        DataTypes.FIELD("date_value", DataTypes.DATE()),
                        DataTypes.FIELD("decimal_value", DataTypes.DECIMAL(10, 2)),
                        DataTypes.FIELD("ts_value", DataTypes.TIMESTAMP(3)))
                .getLogicalType();
        Timestamp timestamp = Timestamp.valueOf(LocalDateTime.of(2026, 5, 1, 10, 30, 15, 123_000_000));

        RowData row = new GreptimeResultSetRowDataConverter(rowType)
                .convert(resultSet(
                        true,
                        (byte) 2,
                        (short) 3,
                        4,
                        5L,
                        1.25f,
                        2.5d,
                        "host-1",
                        new byte[] {1, 2, 3},
                        Date.valueOf("2026-05-01"),
                        new BigDecimal("12.34"),
                        timestamp));

        assertTrue(row.getBoolean(0));
        assertEquals((byte) 2, row.getByte(1));
        assertEquals((short) 3, row.getShort(2));
        assertEquals(4, row.getInt(3));
        assertEquals(5L, row.getLong(4));
        assertEquals(1.25f, row.getFloat(5));
        assertEquals(2.5d, row.getDouble(6));
        assertEquals(StringData.fromString("host-1"), row.getString(7));
        assertArrayEquals(new byte[] {1, 2, 3}, row.getBinary(8));
        assertEquals((int) LocalDate.of(2026, 5, 1).toEpochDay(), row.getInt(9));
        assertEquals(DecimalData.fromBigDecimal(new BigDecimal("12.34"), 10, 2), row.getDecimal(10, 10, 2));
        assertEquals(TimestampData.fromTimestamp(timestamp), row.getTimestamp(11, 3));
    }

    @Test
    void preservesNullFields() throws Exception {
        RowType rowType = (RowType) DataTypes.ROW(
                        DataTypes.FIELD("flag", DataTypes.BOOLEAN()), DataTypes.FIELD("text_value", DataTypes.STRING()))
                .getLogicalType();

        RowData row = new GreptimeResultSetRowDataConverter(rowType).convert(resultSet(null, null));

        assertTrue(row.isNullAt(0));
        assertTrue(row.isNullAt(1));
    }

    @Test
    void rejectsDecimalOverflow() {
        RowType rowType = (RowType) DataTypes.ROW(DataTypes.FIELD("amount", DataTypes.DECIMAL(5, 2)))
                .getLogicalType();

        SQLException error = assertThrows(SQLException.class, () -> new GreptimeResultSetRowDataConverter(rowType)
                .convert(resultSet(new BigDecimal("1234.56"))));

        assertTrue(error.getMessage().contains("DECIMAL value exceeds declared precision/scale"));
        assertTrue(error.getMessage().contains("column=amount"));
        assertTrue(error.getMessage().contains("declaredPrecision=5"));
        assertTrue(error.getMessage().contains("declaredScale=2"));
    }

    @Test
    void supportsEmptyProjection() throws Exception {
        RowType rowType = (RowType) DataTypes.ROW().getLogicalType();

        RowData row = new GreptimeResultSetRowDataConverter(rowType).convert(resultSet());

        assertEquals(0, row.getArity());
    }

    @Test
    void rejectsUnsupportedTypes() {
        RowType rowType = (RowType)
                DataTypes.ROW(DataTypes.FIELD("time_value", DataTypes.TIME())).getLogicalType();

        IllegalArgumentException error =
                assertThrows(IllegalArgumentException.class, () -> new GreptimeResultSetRowDataConverter(rowType));

        assertTrue(error.getMessage().contains("Unsupported GreptimeDB source field type"));
        assertTrue(error.getMessage().contains("time_value"));
    }

    @Test
    void rejectsTimestampWithLocalTimeZoneUntilJdbcSemanticsAreVerified() {
        RowType rowType = (RowType) DataTypes.ROW(DataTypes.FIELD("ltz_value", DataTypes.TIMESTAMP_LTZ(3)))
                .getLogicalType();

        IllegalArgumentException error =
                assertThrows(IllegalArgumentException.class, () -> new GreptimeResultSetRowDataConverter(rowType));

        assertTrue(error.getMessage().contains("Unsupported GreptimeDB source field type"));
        assertTrue(error.getMessage().contains("ltz_value"));
    }

    private static ResultSet resultSet(Object... values) {
        InvocationHandler handler = new InvocationHandler() {
            private Object lastValue;

            @Override
            public Object invoke(Object proxy, java.lang.reflect.Method method, Object[] args) {
                String name = method.getName();
                if ("wasNull".equals(name)) {
                    return lastValue == null;
                }
                if (name.startsWith("get") && args != null && args.length == 1 && args[0] instanceof Integer) {
                    lastValue = values[((Integer) args[0]) - 1];
                    return convertReturnValue(method.getReturnType(), lastValue);
                }
                if ("toString".equals(name)) {
                    return "ResultSetProxy";
                }
                throw new UnsupportedOperationException(name);
            }
        };
        return (ResultSet) Proxy.newProxyInstance(
                GreptimeResultSetRowDataConverterTest.class.getClassLoader(),
                new Class<?>[] {ResultSet.class},
                handler);
    }

    private static Object convertReturnValue(Class<?> returnType, Object value) {
        if (value != null) {
            return value;
        }
        if (returnType == boolean.class) {
            return false;
        }
        if (returnType == byte.class) {
            return (byte) 0;
        }
        if (returnType == short.class) {
            return (short) 0;
        }
        if (returnType == int.class) {
            return 0;
        }
        if (returnType == long.class) {
            return 0L;
        }
        if (returnType == float.class) {
            return 0f;
        }
        if (returnType == double.class) {
            return 0d;
        }
        return null;
    }
}
