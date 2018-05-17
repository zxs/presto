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
import com.facebook.presto.plugin.jdbc.ConnectionFactory;
import com.facebook.presto.plugin.jdbc.DriverConnectionFactory;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.plugin.jdbc.JdbcTypeHandle;
import com.facebook.presto.plugin.jdbc.ReadMapping;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.Decimals;
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

//import java.io.FileWriter;
//import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.plugin.jdbc.DriverConnectionFactory.basicConnectionProperties;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.bigintReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.booleanReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.charReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.dateReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.decimalReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.doubleReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.integerReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.realReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.smallintReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.timeReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.timestampReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.tinyintReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.varbinaryReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.varcharReadMapping;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.CharType.createCharType;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.Math.max;
import static java.lang.Math.min;

public class PhoenixClient
        extends BaseJdbcClient
{
    private static final Logger log = Logger.get(PhoenixClient.class);
    private static final Integer INF = -1;
    private static int duration = 10;
    private LoadingCache<Object, Object> phoenixMetadataCache;

    @Inject
    public PhoenixClient(JdbcConnectorId connectorId, BaseJdbcConfig config)
            throws SQLException
    {
        super(connectorId, config, "\"", connectionFactory(config));
        if (config.getConnectionInfo() != null) {
            try {
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

    private static ConnectionFactory connectionFactory(BaseJdbcConfig config)
            throws SQLException
    {
        Properties connectionProperties = basicConnectionProperties(config);
        if (config.getConnectionInfo() != null) {
            try {
                /*
                String hbaseSite4Phoenix = Resources.getResource("hbase-site.xml").getFile();
                log.info("Generated hbase-site.xml Path: {}", hbaseSite4Phoenix);
                */
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
                /*
                try (FileWriter fileWriter = new FileWriter(hbaseSite4Phoenix)) {
                    conf.writeXml(fileWriter);
                }
                catch (IOException ex) {
                    log.error("N/A Write Phoenix Properties to hbase-site.xml", ex);
                }
                */
                duration = conf.getInt("phoenix.metadata.cache.retention.minutes", 10);
            }
            catch (Exception e) {
                throw Throwables.propagate(new SQLException("!!!read connection-info!!!", e));
            }
        }
        return new DriverConnectionFactory(new PhoenixDriver(), config.getConnectionUrl(), connectionProperties);
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
        try (Connection connection = connectionFactory.openConnection();
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
        try (Connection connection = connectionFactory.openConnection()) {
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
        try (Connection connection = connectionFactory.openConnection()) {
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
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        return (List<JdbcColumnHandle>) phoenixMetadataCache.getUnchecked(tableHandle);
    }

    private List<JdbcColumnHandle> makeColumns(JdbcTableHandle tableHandle)
    {
        return super.getColumns(null, tableHandle);
    }
    @Override
    public Optional<ReadMapping> toPrestoType(ConnectorSession session, JdbcTypeHandle type)
    {
        int columnSize = type.getColumnSize();
        switch (type.getJdbcType()) {
            case Types.BIT:
            case Types.BOOLEAN:
                return Optional.of(booleanReadMapping());

            case Types.TINYINT:
                return Optional.of(tinyintReadMapping());

            case Types.SMALLINT:
                return Optional.of(smallintReadMapping());

            case Types.INTEGER:
                return Optional.of(integerReadMapping());

            case Types.BIGINT:
                return Optional.of(bigintReadMapping());

            case Types.REAL:
                return Optional.of(realReadMapping());

            case Types.FLOAT:
            case Types.DOUBLE:
                return Optional.of(doubleReadMapping());

            case Types.NUMERIC:
            case Types.DECIMAL:
                int decimalDigits = type.getDecimalDigits();
                int precision = columnSize + max(-decimalDigits, 0); // Map decimal(p, -s) (negative scale) to decimal(p+s, 0).
                if (precision > Decimals.MAX_PRECISION) {
                    return Optional.empty();
                }
                return Optional.of(decimalReadMapping(createDecimalType(precision, max(decimalDigits, 0))));

            case Types.CHAR:
            case Types.NCHAR:
                // TODO this is wrong, we're going to construct malformed Slice representation if source > charLength
                int charLength = min(columnSize, CharType.MAX_LENGTH);
                return Optional.of(charReadMapping(createCharType(charLength)));

            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                // phoenix varchar type is unbounded , but columnSize is 0
                // modified @zxs
                if (columnSize > VarcharType.MAX_LENGTH || columnSize == 0) {
                    return Optional.of(varcharReadMapping(createUnboundedVarcharType()));
                }
                return Optional.of(varcharReadMapping(createVarcharType(columnSize)));

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return Optional.of(varbinaryReadMapping());

            case Types.DATE:
                return Optional.of(dateReadMapping());

            case Types.TIME:
                return Optional.of(timeReadMapping());

            case Types.TIMESTAMP:
                return Optional.of(timestampReadMapping());
        }
        return Optional.empty();
    }
}
