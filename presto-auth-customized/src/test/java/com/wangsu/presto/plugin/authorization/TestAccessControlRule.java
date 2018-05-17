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

import com.facebook.presto.spi.security.SystemAccessControl;
import com.google.common.collect.ImmutableMap;
import com.wangsu.presto.plugin.authorization.rule.AccessControlRule;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by sunwl on 2018/4/4.
 */
public class TestAccessControlRule
{
    private static final String ZK_CONNECTION_STRING = "180.101.103.184:50000";
    private String config = "src/test/resources/authorization_rules.json";
    private static final String PATH = "/presto/security/authorization_rules.json";

    public void setData()
            throws Exception
    {
        TestHelper.setZkData(config, ZK_CONNECTION_STRING, PATH);
    }

    String methodName()
    {
        return Thread.currentThread().getStackTrace()[2].getMethodName();
    }

    public void testUserAccessControlRuleModuleFromZk()
    {
        System.out.println(methodName());
        CustomizedSystemAccessControlFactory accessFactory = new CustomizedSystemAccessControlFactory();
        Map<String, String> config = ImmutableMap.<String, String>builder()
                .put("security.config-provider",
                        "com.wangsu.presto.plugin.authorization.rule.ZkBasedRuleFactory")
                .put("security.zk-connection-string", ZK_CONNECTION_STRING)
                .put("security.zk-path", "/presto/security")
                .build();
        SystemAccessControl control = accessFactory.create(config);
        //control.checkCanCreateSchema(new Identity("admin", Optional.empty()), new CatalogSchemaName("a", "b"));
        AccessControlRule rules = ((CustomizedSystemAccessControl) control).getRule();

        assertTrue(rules.accept("gdp", "hive", "default", "hehe"));
        assertTrue(rules.accept("gdp", "hive", "default", "haha"));
        assertFalse(rules.accept("gdp", "hive", "com_cnc_dcc", "haha"));
        assertTrue(rules.accept("gdp", "mysql", "default", "hehe"));
        assertTrue(rules.accept("gdp", "mysql", "default", "haha"));
        assertTrue(rules.accept("gdp", "mysql", "information_schema", "e"));
        assertTrue(rules.accept("gdp", "phoenix", "test", "table"));
        assertFalse(rules.accept("gdp", "phoenix", "test", "table1"));
        assertFalse(rules.accept("gdp", "phoenix", "test1", "hehe"));
        assertFalse(rules.accept("gdp", "kk", "test", "table1"));
        assertTrue(rules.accept("myview", "hive", "com_cnc_dcc", "realChanFlow"));
        assertFalse(rules.accept("myview", "hive", "com_cnc_dcc", "realChanFlow1"));

        assertTrue(rules.acceptCatalog("myview", "hive"));
        assertFalse(rules.acceptCatalog("myview", "hive1"));
        assertTrue(rules.acceptSchema("myview", "hive", "com_cnc_dcc"));

        assertTrue(rules.accept("superman", "hive", "default", "hehe"));
        assertTrue(rules.accept("superman", "hive", "default", "haha"));
        assertTrue(rules.accept("superman", "hive", "com_cnc_dcc", "haha"));
        assertTrue(rules.accept("superman", "mysql", "default", "hehe"));
        assertTrue(rules.accept("superman", "mysql", "default", "haha"));
        assertTrue(rules.accept("superman", "mysql", "information_schema", "e"));
        assertTrue(rules.accept("superman", "phoenix", "test", "table"));
        assertTrue(rules.accept("superman", "phoenix", "test", "table1"));
        assertTrue(rules.accept("superman", "kk", "test", "table1"));
        assertTrue(rules.accept("superman", "hive", "com_cnc_dcc", "realChanFlow"));
        assertTrue(rules.accept("superman", "hive", "com_cnc_dcc", "realChanFlow1"));

        assertTrue(rules.accept("admin", "hive", "default", "hehe"));
        assertTrue(rules.accept("admin", "hive", "default", "haha"));
        assertTrue(rules.accept("admin", "hive", "com_cnc_dcc", "haha"));
        assertTrue(rules.accept("admin", "mysql", "default", "hehe"));
        assertTrue(rules.accept("admin", "mysql", "default", "haha"));
        assertTrue(rules.accept("admin", "mysql", "information_schema", "e"));
        assertTrue(rules.accept("admin", "phoenix", "test", "table"));
        assertTrue(rules.accept("admin", "phoenix", "test", "table1"));
        assertTrue(rules.accept("admin", "kk", "test", "table1"));
        assertTrue(rules.accept("admin", "hive", "com_cnc_dcc", "realChanFlow"));
        assertTrue(rules.accept("admin", "hive", "com_cnc_dcc", "realChanFlow1"));

        assertTrue(rules.accept("yourview", "hive", "com_cnc_dcc", "attackwss"));
        assertTrue(rules.accept("yourview", "hive", "com_cnc_dcc", "attackwss__1"));
        assertTrue(rules.accept("yourview", "hive", "com_cnc_dcc", "attackwss__1"));
        assertTrue(rules.accept("yourview", "hive", "com_cnc_dcc", "attackwss__1"));
        assertTrue(rules.accept("yourview", "hive", "com_cnc_dcc", "attackwss__2"));
        assertTrue(rules.accept("yourview", "hive", "com_cnc_dcc", "attackwss__2"));
        assertFalse(rules.accept("yourview", "hive", "com_cnc_dcc", "attackwst__2"));

        assertTrue(rules.accept("gdp", "hive", "default", "hehe"));
        assertTrue(rules.accept("gdp", "hive", "default", "haha"));
        assertFalse(rules.accept("gdp", "hive", "com_cnc_dcc", "haha"));
        assertTrue(rules.accept("gdp", "mysql", "default", "hehe"));
        assertTrue(rules.accept("gdp", "mysql", "default", "haha"));
        assertTrue(rules.accept("gdp", "mysql", "information_schema", "e"));
        assertTrue(rules.accept("gdp", "phoenix", "test", "table"));
        assertFalse(rules.accept("gdp", "phoenix", "test", "table1"));
        assertFalse(rules.accept("gdp", "phoenix", "test1", "hehe"));
        assertFalse(rules.accept("gdp", "kk", "test", "table1"));
        assertTrue(rules.accept("myview", "hive", "com_cnc_dcc", "realChanFlow"));
        assertFalse(rules.accept("myview", "hive", "com_cnc_dcc", "realChanFlow1"));

        assertTrue(rules.acceptCatalog("myview", "hive"));
        assertFalse(rules.acceptCatalog("myview", "hive1"));
        assertTrue(rules.acceptSchema("myview", "hive", "com_cnc_dcc"));

        assertTrue(rules.accept("superman", "hive", "default", "hehe"));
        assertTrue(rules.accept("superman", "hive", "default", "haha"));
        assertTrue(rules.accept("superman", "hive", "com_cnc_dcc", "haha"));
        assertTrue(rules.accept("superman", "mysql", "default", "hehe"));
        assertTrue(rules.accept("superman", "mysql", "default", "haha"));
        assertTrue(rules.accept("superman", "mysql", "information_schema", "e"));
        assertTrue(rules.accept("superman", "phoenix", "test", "table"));
        assertTrue(rules.accept("superman", "phoenix", "test", "table1"));
        assertTrue(rules.accept("superman", "kk", "test", "table1"));
        assertTrue(rules.accept("superman", "hive", "com_cnc_dcc", "realChanFlow"));
        assertTrue(rules.accept("superman", "hive", "com_cnc_dcc", "realChanFlow1"));
        assertTrue(rules.accept("changed", "hive", "com_cnc_dcc", "realChanFlow1"));
    }

    @Test
    public void testUserAccessControlRuleModule()
    {
        CustomizedSystemAccessControlFactory accessFactory = new CustomizedSystemAccessControlFactory();
        Map<String, String> config = ImmutableMap.<String, String>builder()
                .put("security.config-file", "src/test/resources/authorization_rules.json")
                .put("security.config-provider",
                        "com.wangsu.presto.plugin.authorization.rule.FileBasedRuleFactory")
                .put("security.scheduled-seconds", "10")
                .build();
        AccessControlRule rules = ((CustomizedSystemAccessControl) accessFactory.create(config)).getRule();

        assertTrue(rules.accept("gdp", "hive", "default", "hehe"));
        assertTrue(rules.accept("gdp", "hive", "default", "haha"));
        assertFalse(rules.accept("gdp", "hive", "com_cnc_dcc", "haha"));
        assertTrue(rules.accept("gdp", "mysql", "default", "hehe"));
        assertTrue(rules.accept("gdp", "mysql", "default", "haha"));
        assertTrue(rules.accept("gdp", "mysql", "information_schema", "e"));
        assertTrue(rules.accept("gdp", "phoenix", "test", "table"));
        assertFalse(rules.accept("gdp", "phoenix", "test", "table1"));
        assertFalse(rules.accept("gdp", "phoenix", "test1", "hehe"));
        assertFalse(rules.accept("gdp", "kk", "test", "table1"));
        assertTrue(rules.accept("myview", "hive", "com_cnc_dcc", "realChanFlow"));
        assertFalse(rules.accept("myview", "hive", "com_cnc_dcc", "realChanFlow1"));

        assertTrue(rules.acceptCatalog("myview", "hive"));
        assertFalse(rules.acceptCatalog("myview", "hive1"));
        assertTrue(rules.acceptSchema("myview", "hive", "com_cnc_dcc"));

        assertTrue(rules.accept("superman", "hive", "default", "hehe"));
        assertTrue(rules.accept("superman", "hive", "default", "haha"));
        assertTrue(rules.accept("superman", "hive", "com_cnc_dcc", "haha"));
        assertTrue(rules.accept("superman", "mysql", "default", "hehe"));
        assertTrue(rules.accept("superman", "mysql", "default", "haha"));
        assertTrue(rules.accept("superman", "mysql", "information_schema", "e"));
        assertTrue(rules.accept("superman", "phoenix", "test", "table"));
        assertTrue(rules.accept("superman", "phoenix", "test", "table1"));
        assertTrue(rules.accept("superman", "kk", "test", "table1"));
        assertTrue(rules.accept("superman", "hive", "com_cnc_dcc", "realChanFlow"));
        assertTrue(rules.accept("superman", "hive", "com_cnc_dcc", "realChanFlow1"));

        assertTrue(rules.accept("admin", "hive", "default", "hehe"));
        assertTrue(rules.accept("admin", "hive", "default", "haha"));
        assertTrue(rules.accept("admin", "hive", "com_cnc_dcc", "haha"));
        assertTrue(rules.accept("admin", "mysql", "default", "hehe"));
        assertTrue(rules.accept("admin", "mysql", "default", "haha"));
        assertTrue(rules.accept("admin", "mysql", "information_schema", "e"));
        assertTrue(rules.accept("admin", "phoenix", "test", "table"));
        assertTrue(rules.accept("admin", "phoenix", "test", "table1"));
        assertTrue(rules.accept("admin", "kk", "test", "table1"));
        assertTrue(rules.accept("admin", "hive", "com_cnc_dcc", "realChanFlow"));
        assertTrue(rules.accept("admin", "hive", "com_cnc_dcc", "realChanFlow1"));

        assertTrue(rules.accept("gdp", "hive", "default", "hehe"));
        assertTrue(rules.accept("gdp", "hive", "default", "haha"));
        assertFalse(rules.accept("gdp", "hive", "com_cnc_dcc", "haha"));
        assertTrue(rules.accept("gdp", "mysql", "default", "hehe"));
        assertTrue(rules.accept("gdp", "mysql", "default", "haha"));
        assertTrue(rules.accept("gdp", "mysql", "information_schema", "e"));
        assertTrue(rules.accept("gdp", "phoenix", "test", "table"));
        assertFalse(rules.accept("gdp", "phoenix", "test", "table1"));
        assertFalse(rules.accept("gdp", "phoenix", "test1", "hehe"));
        assertFalse(rules.accept("gdp", "kk", "test", "table1"));
        assertTrue(rules.accept("myview", "hive", "com_cnc_dcc", "realChanFlow"));
        assertFalse(rules.accept("myview", "hive", "com_cnc_dcc", "realChanFlow1"));

        assertTrue(rules.acceptCatalog("myview", "hive"));
        assertFalse(rules.acceptCatalog("myview", "hive1"));
        assertTrue(rules.acceptSchema("myview", "hive", "com_cnc_dcc"));

        assertTrue(rules.accept("superman", "hive", "default", "hehe"));
        assertTrue(rules.accept("superman", "hive", "default", "haha"));
        assertTrue(rules.accept("superman", "hive", "com_cnc_dcc", "haha"));
        assertTrue(rules.accept("superman", "mysql", "default", "hehe"));
        assertTrue(rules.accept("superman", "mysql", "default", "haha"));
        assertTrue(rules.accept("superman", "mysql", "information_schema", "e"));
        assertTrue(rules.accept("superman", "phoenix", "test", "table"));
        assertTrue(rules.accept("superman", "phoenix", "test", "table1"));
        assertTrue(rules.accept("superman", "kk", "test", "table1"));
        assertTrue(rules.accept("superman", "hive", "com_cnc_dcc", "realChanFlow"));
        assertTrue(rules.accept("superman", "hive", "com_cnc_dcc", "realChanFlow1"));
        assertTrue(rules.accept("changed", "hive", "com_cnc_dcc", "realChanFlow1"));
    }
}
