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
package com.wangsu.presto.plugin.authorization;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.airlift.log.Logger;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Created by sunwl on 2018/4/16.
 */
public class IntegrationTest
        extends TestCase
{
    private static final Logger log = Logger.get(IntegrationTest.class);
    /***
     * make sure that it is test environment
     */
    private static final String ZK_CONNECTION_STRING = "180.101.103.184:50000";
    private static final String ZK_PATH = "/presto/security/authorization_rules.json";
    /***
     * based on the below configuration file
     */
    private static final String LOCAL_CONFIG = "src/test/resources/authorization_rules_integration.json";
    /***
     * root user is not admin in below configuration file
     */
    private static final String LOCAL_CONFIG_UPDATE = "src/test/resources/authorization_rules_integration_update.json";
    private static final List<String> USERS = ImmutableList.of("root", "myview", "gdp", "si");
    private static final String PRESTO_URL = "jdbc:presto://180.101.103.194:52000/hive/default";
    private static final String ADMIN = "root";

    public static final String MYSQL_URL = "jdbc:mysql://180.101.103.190:63751/";
    public static final String MYSQL_USER = "watch";
    public static final String MYSQL_PWD = "watch123!@#";

    private static final String SCHEMA1 = "test_auth_1";
    private static final String SCHEMA2 = "test_auth_2";
    private static final String SCHEMA3 = "test_auth_3";

    private Map<String, Connection> conns;
    private Connection mysqlConn;

    private String currentCatalog;

    public IntegrationTest(String name)
    {
        super(name);
    }

    /**
     * Sets up the fixture, for example, open a network connection.
     * This method is called before a test is executed.
     */
    @Override
    protected void setUp()
            throws Exception
    {
        Stopwatch timer = Stopwatch.createStarted();
        log.info("Start to setup");
        setUpConfiguration(LOCAL_CONFIG);
        conns = getPrestoConnections();
        mysqlConn = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PWD);
        prepareTestData(conns.get(ADMIN));
        log.info("Setup completes, elapsed: %ds", timer.elapsed(TimeUnit.SECONDS));
    }

    /**
     * Tears down the fixture, for example, close a network connection.
     * This method is called after a test is executed.
     */
    @Override
    protected void tearDown()
            throws Exception
    {
        conns.forEach((k, v) -> {
            try {
                v.close();
            }
            catch (SQLException e) {
                log.error(e);
            }
        });
        mysqlConn.close();
    }

    public void testGdp()
            throws Exception
    {
        currentCatalog = "hive";
        final String name = "gdp";
        Connection conn = conns.get(name);
        Statement stmt = conn.createStatement();
        canSelect(stmt, SCHEMA1, "t1", name);
        canSelect(stmt, SCHEMA1, "t11", name);
        canSelect(stmt, SCHEMA1, "t2", name);
        canSelect(stmt, SCHEMA1, "t3", name);

        canSelect(stmt, SCHEMA2, "t1", name);
        canSelect(stmt, SCHEMA2, "t11", name);
        canSelect(stmt, SCHEMA2, "t2", name);
        canSelect(stmt, SCHEMA2, "t3", name);

        canSelect(stmt, SCHEMA3, "t1", name);
        canSelect(stmt, SCHEMA3, "t11", name);
        canSelect(stmt, SCHEMA3, "t2", name);
        canSelect(stmt, SCHEMA3, "t3", name);

        deny(stmt, f("create schema %s", "test_auth_123"), "create schema");
        deny(stmt, f("drop schema %s", SCHEMA1), "drop schema");
        deny(stmt, f("create table %s.%s(name varchar)", SCHEMA1, "test_auth_123"), "create table");
        deny(stmt, f("drop table %s.%s", SCHEMA1, "t1"), "drop table");
        deny(stmt, f("alter table %s.%s rename column name to name1", SCHEMA1, "t1"), "rename column");
        deny(stmt, f("alter table %s.%s drop column name", SCHEMA1, "t1"), "drop column");
        deny(stmt, f("create view %s.%s.%s as select * from %s.%s.%s", currentCatalog, SCHEMA2, "t1_abc", currentCatalog, SCHEMA2, "t1"),
                "create view");
        deny(stmt, f("create view %s.%s.%s as select * from %s.%s.%s", currentCatalog, SCHEMA2, "t1_abc", currentCatalog, SCHEMA2, "t1_view"),
                "create view");

        setUpConfiguration(LOCAL_CONFIG_UPDATE);
        /***
         * lost all privileges when change to another config
         */
        denySelect(stmt, SCHEMA1, "t1", name);
        denySelect(stmt, SCHEMA1, "t11", name);
        denySelect(stmt, SCHEMA1, "t2", name);
        denySelect(stmt, SCHEMA1, "t3", name);

        denySelect(stmt, SCHEMA2, "t1", name);
        denySelect(stmt, SCHEMA2, "t11", name);
        denySelect(stmt, SCHEMA2, "t2", name);
        denySelect(stmt, SCHEMA2, "t3", name);

        denySelect(stmt, SCHEMA3, "t1", name);
        denySelect(stmt, SCHEMA3, "t11", name);
        denySelect(stmt, SCHEMA3, "t2", name);
        denySelect(stmt, SCHEMA3, "t3", name);

        setUpConfiguration(LOCAL_CONFIG);
    }

    public void testRoot()
            throws SQLException
    {
        currentCatalog = "hive";
        final String name = "root";
        Connection conn = conns.get(name);
        Statement stmt = conn.createStatement();
        canSelect(stmt, SCHEMA1, "t1", name);
        canSelect(stmt, SCHEMA1, "t11", name);
        canSelect(stmt, SCHEMA1, "t2", name);
        canSelect(stmt, SCHEMA1, "t3", name);

        canSelect(stmt, SCHEMA2, "t1", name);
        canSelect(stmt, SCHEMA2, "t11", name);
        canSelect(stmt, SCHEMA2, "t2", name);
        canSelect(stmt, SCHEMA2, "t3", name);

        canSelect(stmt, SCHEMA3, "t1", name);
        canSelect(stmt, SCHEMA3, "t11", name);
        canSelect(stmt, SCHEMA3, "t2", name);
        canSelect(stmt, SCHEMA3, "t3", name);

        currentCatalog = "mysql";
        /***
         * although user(root) is allowed to create view by customized rule, it is denied by mysql connector
         */
        deny(stmt, f("create view %s.%s.%s as select * from %s.%s.%s", currentCatalog, SCHEMA2, "t1_abc", currentCatalog, SCHEMA2, "t1"),
                "This connector does not support");

        /***
         * although user(root) is allowed to drop table by customized rule, it is denied by hive connector
         */
        deny(stmt, f("drop table %s.test_auth_1.t1", currentCatalog),
                "is disabled in this catalog");
    }

    public void testMyview()
            throws Exception
    {
        final String name = "myview";

        /***
         * hive connector
         */
        currentCatalog = "hive";
        Connection conn = conns.get(name);
        Statement stmt = conn.createStatement();

        canAccessSchema(stmt, SCHEMA1, name);
        canSelect(stmt, SCHEMA1, "t1", name);
        denySelect(stmt, SCHEMA1, "t2", name);
        denySelect(stmt, SCHEMA1, "t3", name);
        denySelect(stmt, SCHEMA1, "t1_view", name);

        canAccessSchema(stmt, SCHEMA2, name);
        canSelect(stmt, SCHEMA2, "t1", name);
        canSelect(stmt, SCHEMA2, "t11", name);
        denySelect(stmt, SCHEMA2, "t2", name);
        denySelect(stmt, SCHEMA2, "t3", name);
        canSelect(stmt, SCHEMA2, "t1_view", name);

        denyAccessSchema(stmt, SCHEMA3, name);
        denySelect(stmt, SCHEMA3, "t1", name);
        denySelect(stmt, SCHEMA3, "t11", name);
        denySelect(stmt, SCHEMA3, "t2", name);
        denySelect(stmt, SCHEMA3, "t3", name);
        denySelect(stmt, SCHEMA3, "t1_view", name);

        deny(stmt, f("create schema %s", "test_auth_123"), "create schema");
        deny(stmt, f("drop schema %s", SCHEMA1), "drop schema");
        deny(stmt, f("create table %s.%s(name varchar)", SCHEMA1, "test_auth_123"), "create table");
        deny(stmt, f("drop table %s.%s", SCHEMA1, "t1"), "drop table");
        deny(stmt, f("show schemas from default"), "access catalog");
        deny(stmt, f("alter table %s.%s rename column name to name1", SCHEMA1, "t1"), "rename column");
        deny(stmt, f("alter table %s.%s drop column name", SCHEMA1, "t1"), "drop column");
        deny(stmt, f("create view %s.%s.%s as select * from %s.%s.%s", currentCatalog, SCHEMA2, "t1_abc", currentCatalog, SCHEMA2, "t1"), "create view");
        deny(stmt, f("create view %s.%s.%s as select * from %s.%s.%s", currentCatalog, SCHEMA2, "t1_abc", currentCatalog, SCHEMA2, "t1_view"), "create view");

        setUpConfiguration(LOCAL_CONFIG_UPDATE);
        /***
         * user myview has no permission to view
         */
        denySelect(stmt, SCHEMA1, "t1_view", name);
        /***
         * ============================================================================================
         * user myview has permission to view(t1_view) but the definer(root) of view has lost the permission to the table(t1)
         * ============================================================================================
         */
        denySelect(stmt, SCHEMA2, "t1_view", "t1", "root");
        /***
         * user myview has no permission to view
         */
        denySelect(stmt, SCHEMA3, "t1_view", name);
        setUpConfiguration(LOCAL_CONFIG);

        /***
         * mysql connector
         */
        currentCatalog = "mysql";

        canAccessSchema(stmt, SCHEMA1, name);
        canSelect(stmt, SCHEMA1, "t1", name);
        canSelect(stmt, SCHEMA1, "t2", name);
        canSelect(stmt, SCHEMA1, "t3", name);

        canAccessSchema(stmt, SCHEMA2, name);
        canSelect(stmt, SCHEMA2, "t1", name);
        canSelect(stmt, SCHEMA2, "t11", name);
        canSelect(stmt, SCHEMA2, "t2", name);
        canSelect(stmt, SCHEMA2, "t3", name);

        canAccessSchema(stmt, SCHEMA3, name);
        canSelect(stmt, SCHEMA3, "t1", name);
        denySelect(stmt, SCHEMA3, "t11", name);
        denySelect(stmt, SCHEMA3, "t2", name);
        denySelect(stmt, SCHEMA3, "t3", name);

        deny(stmt, f("create schema %s", "test_auth_123"), "create schema");
        deny(stmt, f("drop schema %s", SCHEMA1), "drop schema");
        deny(stmt, f("create table %s.%s(name varchar)", SCHEMA1, "test_auth_123"), "create table");
        deny(stmt, f("drop table %s.%s", SCHEMA1, "t1"), "drop table");
        deny(stmt, f("show schemas from default"), "access catalog");
        deny(stmt, f("alter table %s.%s rename column name to name1", SCHEMA1, "t1"), "rename column");
        deny(stmt, f("alter table %s.%s drop column name", SCHEMA1, "t1"), "drop column");
        deny(stmt, f("create view %s.%s.%s as select * from %s.%s.%s", currentCatalog, SCHEMA2, "t1_abc", currentCatalog, SCHEMA2, "t1"), "create view");
        deny(stmt, f("create view %s.%s.%s as select * from %s.%s.%s", currentCatalog, SCHEMA2, "t1_abc", currentCatalog, SCHEMA2, "t1_view"), "create view");
    }

    private void canAccessSchema(Statement stmt, String schema, String user)
            throws SQLException
    {
        stmt.execute(f("show tables from %s.%s", currentCatalog, schema));
    }

    private void deny(Statement stmt, String sql, String message)
    {
        try {
            stmt.execute(sql);
            fail("Expected to fail");
        }
        catch (Exception e) {
            assertTrue(e instanceof SQLException);
            log.info(e.getMessage());
            assertTrue(e.getMessage().contains(message));
        }
    }

    private void denyAccessSchema(Statement stmt, String schema, String user)
            throws SQLException
    {
        try {
            stmt.execute(f("show tables from %s.%s", currentCatalog, schema));
            fail("Expected to fail");
        }
        catch (Exception e) {
            assertTrue(e instanceof SQLException);
            assertTrue(e.getMessage().contains(f("User [%s] is not allowed to show the table metadata in [%s.%s]", user, currentCatalog, schema)));
        }
    }

    private void canSelect(Statement stmt, String schema, String table, String user)
            throws SQLException
    {
        stmt.execute(f("select * from %s.%s.%s", currentCatalog, schema, table));
    }

    private void denySelect(Statement stmt, String schema, String t1, String user)
            throws SQLException
    {
        denySelect(stmt, schema, t1, t1, user);
    }

    private void denySelect(Statement stmt, String schema, String t1, String t2, String user)
            throws SQLException
    {
        try {
            stmt.execute(f("select * from %s.%s.%s", currentCatalog, schema, t1));
            fail("Expected to fail");
        }
        catch (Exception e) {
            assertTrue(e instanceof SQLException);
            log.info(e.getMessage());
            assertTrue(e.getMessage().contains(f("User [%s] is not allowed to select from %s.%s.%s", user, currentCatalog, schema, t2)));
        }
    }

    private void setUpConfiguration(String config)
            throws Exception
    {
        TestHelper.setZkData(config, ZK_CONNECTION_STRING, ZK_PATH);
        TimeUnit.SECONDS.sleep(1);
    }

    private Map<String, Connection> getPrestoConnections()
            throws SQLException
    {
        return USERS.stream().map(user -> {
            try {
                return Maps.immutableEntry(user, DriverManager.getConnection(PRESTO_URL, user, null));
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private void prepareTestData(Connection conn)
            throws SQLException
    {
        ImmutableMap.of("hive", conn.createStatement(), "mysql", mysqlConn.createStatement()).forEach((catalog, stmt) -> {
            try {
                currentCatalog = catalog;
                createSchema(stmt, SCHEMA1);
                createTable(stmt, SCHEMA1, "t1");
                createTable(stmt, SCHEMA1, "t11");
                createTable(stmt, SCHEMA1, "t2");
                createTable(stmt, SCHEMA1, "t3");
                //createView(stmt, SCHEMA1, "t1_view");

                createSchema(stmt, SCHEMA2);
                createTable(stmt, SCHEMA2, "t1");
                createTable(stmt, SCHEMA2, "t11");
                createTable(stmt, SCHEMA2, "t2");
                createTable(stmt, SCHEMA2, "t3");
                //createView(stmt, SCHEMA2, "t1_view");

                createSchema(stmt, SCHEMA3);
                createTable(stmt, SCHEMA3, "t1");
                createTable(stmt, SCHEMA3, "t11");
                createTable(stmt, SCHEMA3, "t2");
                createTable(stmt, SCHEMA3, "t3");
                //createView(stmt, SCHEMA3, "t1_view");
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        Statement stmt = conn.createStatement();
        currentCatalog = "hive";
        createView(stmt, SCHEMA1, "t1_view");
        createView(stmt, SCHEMA2, "t1_view");
        createView(stmt, SCHEMA3, "t1_view");
    }

    private void createSchema(Statement stmt, String name)
            throws SQLException
    {
        //stmt.execute(f("drop schema if exists %s cascade", name));
        if (Objects.equals(currentCatalog, "mysql")) {
            stmt.execute(f("create schema if not exists %s", name));
        }
        else {
            stmt.execute(f("create schema if not exists %s.%s", currentCatalog, name));
        }
    }

    private void createView(Statement stmt, String schema, String view)
            throws SQLException
    {
        try {
            stmt.execute(f("create view %s.%s.%s as select * from %s.%s.%s", currentCatalog, schema, view,
                    currentCatalog, schema, "t1"));
        }
        catch (Exception e) {
            log.info(e.getMessage());
        }
    }

    private void createTable(Statement stmt, String schema, String table)
            throws SQLException
    {
        String name;
        if (Objects.equals(currentCatalog, "mysql")) {
            name = String.join(".", schema, table);
        }
        else {
            name = String.join(".", currentCatalog, schema, table);
        }
        stmt.execute(f("create table if not exists %s(name varchar(32))", name));
        stmt.execute(f("delete from %s", name));
        stmt.execute(f("insert into %s values('%s')", name, name));
    }

    private String f(String sql, String... args)
    {
        return String.format(sql, args);
    }

    public void test()
            throws Exception
    {
        testRoot();
        testGdp();
        testMyview();
    }

    public static Test suite()
    {
        TestSuite suite = new TestSuite();
        suite.addTest(new IntegrationTest("test"));
        return suite;
    }
}
