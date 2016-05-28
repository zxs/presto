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
package com.facebook.presto.plugin.impala;

import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcSplit;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.hive.jdbc.HiveDriver;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Locale.ENGLISH;

public class ImpalaClient
        extends BaseJdbcClient
{
    @Inject
    public ImpalaClient(JdbcConnectorId connectorId, BaseJdbcConfig config)
            throws SQLException
    {
        super(connectorId, config, "`", new HiveDriver());
        /*
        try {
            Configuration conf = new Configuration(false);
            if (config.getConnectionInfo() != null) {
                for (String c : config.getConnectionInfo()) {
                    conf.addResource(new Path(c));
                }
            }
            UserGroupInformation.setConfiguration(conf);
            if (UserGroupInformation.isSecurityEnabled()) {
                connectionProperties.setProperty("auth", "sasl");
                UserGroupInformation.loginUserFromKeytab(config.getConnectionUser(), config.getConnectionPassword());
                final long checkInterval = conf.getLong("hadoop.security.checkIntervalHours", 24) * 60 * 60 * 1000L;
                new Timer(true).scheduleAtFixedRate(new TimerTask()
                                                    {
                                                        @Override
                                                        public void run()
                                                        {
                                                            try {
                                                                UserGroupInformation.getCurrentUser().checkTGTAndReloginFromKeytab();
                                                            }
                                                            catch (IOException e) {
                                                                throw Throwables.propagate(new SQLException("!!!checkTGTAndReloginFromKeytab!!!", e));
                                                            }
                                                        }
                                                    },
                        checkInterval,
                        checkInterval
                );
            }
        }
        catch (Exception e) {
            throw Throwables.propagate(new SQLException("!!!loginUserFromKeytab!!!", e));
        }
        */
    }

    /*
    * skip internal schemas
     * <ul>
    * <li>cloudera_manager_metastore_canary_test_db</li>
    * <li>_impala_builtins</li>
    * </ul>
    * */
    @Override
    public Set<String> getSchemaNames()
    {
        try (Connection connection = driver.connect(connectionUrl, connectionProperties);
             ResultSet resultSet = connection.getMetaData().getSchemas()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_SCHEM").toLowerCase(ENGLISH);
                // skip internal schemas
                if (schemaName.startsWith("cloudera_manager_metastore_canary_test_db")
                        || schemaName.equals("_impala_builtins")) {
                    continue;
                }
                schemaNames.add(schemaName);
            }
            return schemaNames.build();
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override/*NOT SUPPORT storesUpperCaseIdentifiers METHOD*/
    public List<SchemaTableName> getTableNames(String schema)
    {
        try (Connection connection = driver.connect(connectionUrl, connectionProperties)) {
            try (ResultSet resultSet = getTables(connection, schema, null)) {
                ImmutableList.Builder<SchemaTableName> list = ImmutableList.builder();
                while (resultSet.next()) {
                    SchemaTableName schemaTableName = getSchemaTableName(resultSet);
                    // skip internal schemas
                    if (schemaTableName.getSchemaName().startsWith("cloudera_manager_metastore_canary_test_db")
                            || schemaTableName.getSchemaName().equals("_impala_builtins")) {
                        continue;
                    }
                    list.add(schemaTableName);
                }
                return list.build();
            }
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

//    @Override
//    protected ResultSet getTables(Connection connection, String schemaName, String tableName)
//            throws SQLException
//    {
//        // MySQL maps their "database" to SQL catalogs and does not have schemas
//        return connection.getMetaData().getTables(schemaName, null, tableName, new String[] {"TABLE"});
//    }

    @Override/*NOT SUPPORT storesUpperCaseIdentifiers METHOD*/
    public JdbcTableHandle getTableHandle(SchemaTableName schemaTableName)
    {
        try (Connection connection = driver.connect(connectionUrl, connectionProperties)) {
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
                return getOnlyElement(tableHandles);
            }
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override /*NOT SUPPORT setReadOnly METHOD*/
    public Connection getConnection(JdbcSplit split)
            throws SQLException
    {
        return driver.connect(split.getConnectionUrl(), toProperties(split.getConnectionProperties()));
    }

    @Override
    protected String toSqlType(Type type)
    {
        String sqlType = super.toSqlType(type);
        switch (sqlType) {
            case "varchar":
                return "mediumtext";
            case "varbinary":
                return "mediumblob";
            case "time with timezone":
                return "time";
            case "timestamp":
            case "timestamp with timezone":
                return "datetime";
        }
        return sqlType;
    }

    private static Properties toProperties(Map<String, String> map)
    {
        Properties properties = new Properties();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            properties.setProperty(entry.getKey(), entry.getValue());
        }
        return properties;
    }
}
