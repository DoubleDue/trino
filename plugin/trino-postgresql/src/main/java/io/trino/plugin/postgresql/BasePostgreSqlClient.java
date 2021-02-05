/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.postgresql;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.BlockReadFunction;
import io.trino.plugin.jdbc.BlockWriteFunction;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.SliceWriteFunction;
import io.trino.plugin.jdbc.StatsCollecting;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import org.postgresql.core.TypeInfo;
import org.postgresql.jdbc.PgConnection;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiFunction;

import static com.fasterxml.jackson.core.JsonFactory.Feature.CANONICALIZE_FIELD_NAMES;
import static com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.Slices.wrappedLongArray;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampColumnMappingUsingSqlTimestamp;
import static io.trino.plugin.postgresql.GaussdbTypeUtils.getJdbcObjectArray;
import static io.trino.plugin.postgresql.GaussdbTypeUtils.jdbcObjectArrayToBlock;
import static io.trino.plugin.postgresql.GaussdbTypeUtils.toBoxedArray;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.sql.DatabaseMetaData.columnNoNulls;

public abstract class BasePostgreSqlClient
        extends BaseJdbcClient
{
    protected static final String DUPLICATE_TABLE_SQLSTATE = "42P07";

    protected final Type jsonType;
    protected final Type uuidType;
    protected final boolean supportArrays;

    public BasePostgreSqlClient(
            BaseJdbcConfig config,
            PostgreSqlConfig postgresqlConfig,
            @StatsCollecting ConnectionFactory connectionFactory,
            TypeManager typeManager)
    {
        super(config, "\"", connectionFactory);
        this.jsonType = typeManager.getType(new TypeSignature(StandardTypes.JSON));
        this.uuidType = typeManager.getType(new TypeSignature(StandardTypes.UUID));

        switch (postgresqlConfig.getArrayMapping()) {
            case DISABLED:
                supportArrays = false;
                break;
            case AS_ARRAY:
                supportArrays = true;
                break;
            default:
                throw new IllegalArgumentException("Unsupported ArrayMapping: " + postgresqlConfig.getArrayMapping());
        }
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        try {
            createTable(session, tableMetadata, tableMetadata.getTable().getTableName());
        }
        catch (SQLException e) {
            boolean exists = DUPLICATE_TABLE_SQLSTATE.equals(e.getSQLState());
            throw new TrinoException(exists ? ALREADY_EXISTS : JDBC_ERROR, e);
        }
    }

    @Override
    protected void renameTable(ConnectorSession session, String catalogName, String schemaName, String tableName, SchemaTableName newTable)
    {
        if (!schemaName.equals(newTable.getSchemaName())) {
            throw new TrinoException(NOT_SUPPORTED, "Table rename across schemas is not supported in PostgreSQL");
        }

        String sql = format(
                "ALTER TABLE %s RENAME TO %s",
                quoted(catalogName, schemaName, tableName),
                quoted(newTable.getTableName()));

        try (Connection connection = connectionFactory.openConnection(session)) {
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql)
            throws SQLException
    {
        connection.setAutoCommit(false);
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setFetchSize(1000);
        return statement;
    }

    @Override
    protected ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        Optional<String> escape = Optional.ofNullable(metadata.getSearchStringEscape());
        return metadata.getTables(
                connection.getCatalog(),
                escapeNamePattern(schemaName, escape.get()).orElse(null),
                escapeNamePattern(tableName, escape.get()).orElse(null),
                new String[] {"TABLE", "VIEW", "MATERIALIZED VIEW", "FOREIGN TABLE"});
    }

    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        try (Connection connection = connectionFactory.openConnection(session)) {
            Map<String, Integer> arrayColumnDimensions = getArrayColumnDimensions(connection, tableHandle);
            try (ResultSet resultSet = getColumns(tableHandle, connection.getMetaData())) {
                List<JdbcColumnHandle> columns = new ArrayList<>();
                while (resultSet.next()) {
                    String columnName = resultSet.getString("COLUMN_NAME");
                    JdbcTypeHandle typeHandle = new JdbcTypeHandle(
                            resultSet.getInt("DATA_TYPE"),
                            Optional.of(resultSet.getString("TYPE_NAME")),
                            resultSet.getInt("COLUMN_SIZE"),
                            resultSet.getInt("DECIMAL_DIGITS"),
                            Optional.ofNullable(arrayColumnDimensions.get(columnName)));
                    Optional<ColumnMapping> columnMapping = toColumnMapping(session, connection, typeHandle);
                    // skip unsupported column types
                    if (columnMapping.isPresent()) {
                        boolean nullable = (resultSet.getInt("NULLABLE") != columnNoNulls);
                        columns.add(new JdbcColumnHandle(columnName, typeHandle, columnMapping.get().getType(), nullable));
                    }
                }
                if (columns.isEmpty()) {
                    // In rare cases a table might have no columns.
                    throw new TableNotFoundException(tableHandle.getSchemaTableName());
                }
                return ImmutableList.copyOf(columns);
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    protected Map<String, Integer> getArrayColumnDimensions(Connection connection, JdbcTableHandle tableHandle)
            throws SQLException
    {
        if (!supportArrays) {
            return ImmutableMap.of();
        }
        String sql = "" +
                "SELECT att.attname, greatest(att.attndims, 1) AS attndims " +
                "FROM pg_attribute att " +
                "  JOIN pg_type attyp ON att.atttypid = attyp.oid" +
                "  JOIN pg_class tbl ON tbl.oid = att.attrelid " +
                "  JOIN pg_namespace ns ON tbl.relnamespace = ns.oid " +
                "WHERE ns.nspname = ? " +
                "AND tbl.relname = ? " +
                "AND attyp.typcategory = 'A' ";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, tableHandle.getSchemaName());
            statement.setString(2, tableHandle.getTableName());

            Map<String, Integer> arrayColumnDimensions = new HashMap<>();
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    arrayColumnDimensions.put(resultSet.getString("attname"), resultSet.getInt("attndims"));
                }
            }
            return arrayColumnDimensions;
        }
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        return super.legacyToWriteMapping(session, type);
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        String jdbcTypeName = typeHandle.getJdbcTypeName()
                .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + typeHandle));

        switch (jdbcTypeName) {
            case "uuid":
                return Optional.of(uuidColumnMapping());
            case "jsonb":
            case "json":
                return Optional.of(jsonColumnMapping());
            case "timestamptz":
                // PostgreSQL's "timestamp with time zone" is reported as Types.TIMESTAMP rather than Types.TIMESTAMP_WITH_TIMEZONE
                return Optional.of(timestampWithTimeZoneColumnMapping());
        }
        if (typeHandle.getJdbcType() == Types.VARCHAR && !jdbcTypeName.equals("varchar")) {
            // This can be e.g. an ENUM
            return Optional.of(typedVarcharColumnMapping(jdbcTypeName));
        }
        if (typeHandle.getJdbcType() == Types.TIMESTAMP) {
            return Optional.of(timestampColumnMappingUsingSqlTimestamp(TIMESTAMP_MILLIS));
        }
        if (typeHandle.getJdbcType() == Types.ARRAY && supportArrays) {
            if (!typeHandle.getArrayDimensions().isPresent()) {
                return Optional.empty();
            }
            JdbcTypeHandle elementTypeHandle = getArrayElementTypeHandle(connection, typeHandle);
            String elementTypeName = typeHandle.getJdbcTypeName()
                    .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Element type name is missing: " + elementTypeHandle));
            if (elementTypeHandle.getJdbcType() == Types.VARBINARY) {
                // PostgreSQL jdbc driver doesn't currently support array of varbinary (bytea[])
                // https://github.com/pgjdbc/pgjdbc/pull/1184
                return Optional.empty();
            }
            return toColumnMapping(session, connection, elementTypeHandle)
                    .map(elementMapping -> {
                        ArrayType prestoArrayType = new ArrayType(elementMapping.getType());
                        int arrayDimensions = typeHandle.getArrayDimensions().get();
                        for (int i = 1; i < arrayDimensions; i++) {
                            prestoArrayType = new ArrayType(prestoArrayType);
                        }
                        return arrayColumnMapping(session, prestoArrayType, elementTypeName);
                    });
        }
        return super.legacyToPrestoType(session, connection, typeHandle);
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> sql + " LIMIT " + limit);
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
        return true;
    }

    protected static ColumnMapping timestampWithTimeZoneColumnMapping()
    {
        return ColumnMapping.longMapping(
                TIMESTAMP_WITH_TIME_ZONE,
                (resultSet, columnIndex) -> {
                    // PostgreSQL does not store zone information in "timestamp with time zone" data type
                    long millisUtc = resultSet.getTimestamp(columnIndex).getTime();
                    return packDateTimeWithZone(millisUtc, UTC_KEY);
                },
                timestampWithTimeZoneWriteFunction());
    }

    protected static LongWriteFunction timestampWithTimeZoneWriteFunction()
    {
        return (statement, index, value) -> {
            // PostgreSQL does not store zone information in "timestamp with time zone" data type
            long millisUtc = unpackMillisUtc(value);
            statement.setTimestamp(index, new java.sql.Timestamp(millisUtc));
        };
    }

    protected static ColumnMapping arrayColumnMapping(ConnectorSession session, ArrayType arrayType, String elementJdbcTypeName)
    {
        return ColumnMapping.blockMapping(
                arrayType,
                arrayReadFunction(session, arrayType.getElementType()),
                arrayWriteFunction(session, arrayType.getElementType(), elementJdbcTypeName));
    }

    protected static BlockReadFunction arrayReadFunction(ConnectorSession session, Type elementType)
    {
        return (resultSet, columnIndex) -> {
            Object[] objectArray = toBoxedArray(resultSet.getArray(columnIndex).getArray());
            return jdbcObjectArrayToBlock(session, elementType, objectArray);
        };
    }

    protected static BlockWriteFunction arrayWriteFunction(ConnectorSession session, Type elementType, String elementJdbcTypeName)
    {
        return (statement, index, block) -> {
            Array jdbcArray = statement.getConnection().createArrayOf(elementJdbcTypeName, getJdbcObjectArray(session, elementType, block));
            statement.setArray(index, jdbcArray);
        };
    }

    protected JdbcTypeHandle getArrayElementTypeHandle(Connection connection, JdbcTypeHandle arrayTypeHandle)
    {
        String jdbcTypeName = arrayTypeHandle.getJdbcTypeName()
                .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + arrayTypeHandle));
        try {
            TypeInfo typeInfo = connection.unwrap(PgConnection.class).getTypeInfo();
            int pgElementOid = typeInfo.getPGArrayElement(typeInfo.getPGType(jdbcTypeName));
            return new JdbcTypeHandle(
                    typeInfo.getSQLType(pgElementOid),
                    Optional.of(typeInfo.getPGType(pgElementOid)),
                    arrayTypeHandle.getColumnSize().get(),
                    arrayTypeHandle.getDecimalDigits().get(),
                    arrayTypeHandle.getArrayDimensions());
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    protected abstract ColumnMapping jsonColumnMapping();

    protected abstract ColumnMapping typedVarcharColumnMapping(String jdbcTypeName);

    protected static SliceWriteFunction uuidWriteFunction()
    {
        return (statement, index, value) -> {
            UUID uuid = new UUID(value.getLong(0), value.getLong(SIZE_OF_LONG));
            statement.setObject(index, uuid, Types.OTHER);
        };
    }

    protected static Slice uuidSlice(UUID uuid)
    {
        return wrappedLongArray(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }

    protected ColumnMapping uuidColumnMapping()
    {
        return ColumnMapping.sliceMapping(
                uuidType,
                (resultSet, columnIndex) -> uuidSlice((UUID) resultSet.getObject(columnIndex)),
                uuidWriteFunction());
    }

    protected static final JsonFactory JSON_FACTORY = new JsonFactory()
            .disable(CANONICALIZE_FIELD_NAMES);

    protected static final ObjectMapper SORTED_MAPPER = new ObjectMapperProvider().get().configure(ORDER_MAP_ENTRIES_BY_KEYS, true);

    protected static Slice jsonParse(Slice slice)
    {
        try (JsonParser parser = createJsonParser(slice)) {
            byte[] in = slice.getBytes();
            SliceOutput dynamicSliceOutput = new DynamicSliceOutput(in.length);
            SORTED_MAPPER.writeValue((OutputStream) dynamicSliceOutput, SORTED_MAPPER.readValue(parser, Object.class));
            // nextToken() returns null if the input is parsed correctly,
            // but will throw an exception if there are trailing characters.
            parser.nextToken();
            return dynamicSliceOutput.slice();
        }
        catch (Exception e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("Cannot convert '%s' to JSON", slice.toStringUtf8()));
        }
    }

    protected static JsonParser createJsonParser(Slice json)
            throws IOException
    {
        // Jackson tries to detect the character encoding automatically when using InputStream
        // so we pass an InputStreamReader instead.
        return JSON_FACTORY.createParser(new InputStreamReader(json.getInput(), UTF_8));
    }
}
