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
package com.facebook.presto.plugin.phoenix;

import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.phoenix.jdbc.PhoenixDriver;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.CharType.createCharType;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.Math.min;

public class PhoenixClient
        extends BaseJdbcClient
{
    private static final Logger log = Logger.get(PhoenixClient.class);
    private static final Integer INF = -1;
    private LoadingCache<Object, Object> phoenixMetadataCache;
    @Inject
    public PhoenixClient(JdbcConnectorId connectorId, BaseJdbcConfig config)
            throws SQLException
    {
        super(connectorId, config, "\"", new PhoenixDriver());
        if (config.getConnectionInfo() != null) {
            try {
                Configuration conf = new Configuration(false);
                if (config.getConnectionInfo() != null) {
                    for (String c : config.getConnectionInfo()) {
                        conf.addResource(new Path(c));
                    }
                }
                Iterator<Map.Entry<String, String>> eItr = conf.iterator();
                Map.Entry<String, String> entry;
                while (eItr.hasNext()) {
                    entry = eItr.next();
                    connectionProperties.put(entry.getKey(), entry.getValue());
                }
                int duration = conf.getInt("phoenix.metadata.cache.retention.minutes", 10);
                // create metadata cache
                phoenixMetadataCache = CacheBuilder.newBuilder()
                    .expireAfterWrite(duration, TimeUnit.MINUTES)
                    .build(createMetadataCacheLoader());
            }
            catch (Exception e) {
                throw Throwables.propagate(new SQLException("!!!read connection-info!!!", e));
            }
        }
    }

    private CacheLoader<Object, Object> createMetadataCacheLoader()
    {
        return new CacheLoader<Object, Object>()
        {
            @Override
            public Object load(Object key) throws Exception
            {
                long t1 = System.currentTimeMillis();
                Object ret = null;
                if (key instanceof Integer) {
                    ret = makeSchemaNames();
                }
                else if (key instanceof String) {
                    ret = makeTableNames(Strings.emptyToNull((String) key));
                }
                else if (key instanceof SchemaTableName) {
                    ret = makeTableHandle((SchemaTableName) key);
                }
                else if (key instanceof JdbcTableHandle) {
                    ret = makeColumns((JdbcTableHandle) key);
                }
                if (ret == null) {
                    throw Throwables.propagate(new SQLException(String.format("!!!Load Phoenix Metadata by key[%s]!!!", key)));
                }
                long t2 = System.currentTimeMillis();
                log.info("==> GET VAL: %s BY KEY: %s, SPEND %s ms", ret, key, t2 - t1);
                return ret;
            }
        };
    }

    @Override
    public Set<String> getSchemaNames()
    {
        return (Set<String>) phoenixMetadataCache.getUnchecked(INF);
    }

    private Set<String> makeSchemaNames()
    {
        try (Connection connection = driver.connect(connectionUrl, connectionProperties);
             ResultSet resultSet = connection.getMetaData().getSchemas()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_SCHEM");
                // skip internal schemas
                if (schemaName != null && !schemaName.equals("SYSTEM")) {
                    schemaNames.add(schemaName);
                }
            }
            return schemaNames.build();
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    protected ResultSet getTables(Connection connection, String schemaName, String tableName)
            throws SQLException
    {
        return connection.getMetaData().getTables(connection.getCatalog(), schemaName, tableName, new String[]{"TABLE", "VIEW"});
    }

    @Override
    public List<SchemaTableName> getTableNames(String schema)
    {
        return (List<SchemaTableName>) phoenixMetadataCache.getUnchecked(Strings.nullToEmpty(schema));
    }

    private List<SchemaTableName> makeTableNames(String schema)
    {
        try (Connection connection = driver.connect(connectionUrl, connectionProperties)) {
            try (ResultSet resultSet = getTables(connection, schema, null)) {
                ImmutableList.Builder<SchemaTableName> list = ImmutableList.builder();
                while (resultSet.next()) {
                    try {
                        list.add(getSchemaTableName(resultSet));
                    }
                    catch (NullPointerException npe) {
                        log.warn("Get SchemaTableName EX", npe);
                    }
                    catch (IllegalArgumentException iae) {
                        log.warn("Get SchemaTableName EX", iae);
                    }
                }
                return list.build();
            }
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public JdbcTableHandle getTableHandle(SchemaTableName schemaTableName)
    {
        return (JdbcTableHandle) phoenixMetadataCache.getUnchecked(schemaTableName);
    }

    private JdbcTableHandle makeTableHandle(SchemaTableName schemaTableName)
    {
        try (Connection connection = driver.connect(connectionUrl, connectionProperties)) {
            //DatabaseMetaData metadata = connection.getMetaData();
            String jdbcSchemaName = schemaTableName.getSchemaName();
            String jdbcTableName = schemaTableName.getTableName();

            try (ResultSet resultSet = getTables(connection, jdbcSchemaName, jdbcTableName)) {
                List<JdbcTableHandle> tableHandles = new ArrayList<>();
                while (resultSet.next()) {
                    tableHandles.add(new JdbcTableHandle(
                            connectorId,
                            schemaTableName,
                            resultSet.getString("TABLE_CAT"),
                            resultSet.getString("TABLE_SCHEM"),
                            resultSet.getString("TABLE_NAME")));
                }
                if (tableHandles.isEmpty()) {
                    return null;
                }
                if (tableHandles.size() > 1) {
                    throw new PrestoException(NOT_SUPPORTED, "Multiple tables matched: " + schemaTableName);
                }
                JdbcTableHandle ret = getOnlyElement(tableHandles);
                return ret;
            }
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public List<JdbcColumnHandle> getColumns(JdbcTableHandle tableHandle)
    {
        return (List<JdbcColumnHandle>) phoenixMetadataCache.getUnchecked(tableHandle);
    }

    private List<JdbcColumnHandle> makeColumns(JdbcTableHandle tableHandle)
    {
        return super.getColumns(tableHandle);
    }

    protected Type toPrestoType(int jdbcType, int columnSize)
    {
        switch (jdbcType) {
            case Types.BIT:
            case Types.BOOLEAN:
                return BOOLEAN;
            case Types.TINYINT:
                return TINYINT;
            case Types.SMALLINT:
                return SMALLINT;
            case Types.INTEGER:
                return INTEGER;
            case Types.BIGINT:
                return BIGINT;
            case Types.REAL:
                return REAL;
            case Types.FLOAT:
            case Types.DOUBLE:
            case Types.NUMERIC:
            case Types.DECIMAL:
                return DOUBLE;
            case Types.CHAR:
            case Types.NCHAR:
                return createCharType(min(columnSize, CharType.MAX_LENGTH));
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                // phoenix varchar type is unbounded , but columnSize is 0
                // modified @zxs
                if (columnSize > VarcharType.MAX_LENGTH || columnSize == 0) {
                    return createUnboundedVarcharType();
                }
                return createVarcharType(columnSize);
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return VARBINARY;
            case Types.DATE:
                return DATE;
            case Types.TIME:
                return TIME;
            case Types.TIMESTAMP:
                return TIMESTAMP;
        }
        return null;
    }
}
