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

import com.facebook.presto.spi.CatalogSchemaName;
import com.facebook.presto.spi.CatalogSchemaTableName;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.Privilege;
import com.facebook.presto.spi.security.SystemAccessControl;
import com.google.common.collect.ImmutableSet;
import com.wangsu.presto.plugin.authorization.rule.AccessControlRule;
import io.airlift.log.Logger;

import java.security.Principal;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by sunwl on 2018/4/4.
 */
public class CustomizedSystemAccessControl
        implements SystemAccessControl
{
    private static final Logger log = Logger.get(CustomizedSystemAccessControl.class);
    private AccessControlRule rule;

    public CustomizedSystemAccessControl(AccessControlRule rule)
    {
        this.rule = rule;
    }

    public AccessControlRule getRule()
    {
        return rule;
    }

    private static final String DEFAULT_INFO = "[%s] -> [%s]";

    private void checkIsAdmin(String userName, String message)
    {
        if (!rule.isAdmin(userName)) {
            throw new AccessDeniedException(message);
        }
    }

    private String methodName()
    {
        return Thread.currentThread().getStackTrace()[2].getMethodName();
    }

    private String notAllow(String user, String action)
    {
        String template = "User [%s] is not allowed to " + action + ".";
        return String.format(template, user);
    }

    /**
     * Check if the principal is allowed to be the specified user.
     *
     * @param principal
     * @param userName
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanSetUser(Principal principal, String userName)
    {
        log.debug(DEFAULT_INFO, userName, methodName());
        //anyone can set user
    }

    /**
     * Check if identity is allowed to set the specified system property.
     *
     * @param identity
     * @param propertyName
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
    {
        log.debug(DEFAULT_INFO, identity.getUser(), methodName());
        checkIsAdmin(identity.getUser(), notAllow(identity.getUser(), "set system session property"));
    }

    /**
     * Check if identity is allowed to access the specified catalog
     *
     * @param identity
     * @param catalogName
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanAccessCatalog(Identity identity, String catalogName)
    {
        log.debug(DEFAULT_INFO, identity.getUser(), methodName());
        String user = identity.getUser();
        if (!rule.acceptCatalog(user, catalogName)) {
            throw new AccessDeniedException(String.format("User [%s] is not allowed to access catalog [%s]",
                    user, catalogName));
        }
    }

    /**
     * Filter the list of catalogs to those visible to the identity.
     *
     * @param identity
     * @param catalogs
     */
    @Override
    public Set<String> filterCatalogs(Identity identity, Set<String> catalogs)
    {
        log.debug(DEFAULT_INFO, identity.getUser(), methodName());
        return ImmutableSet.copyOf(
                catalogs.stream().filter(c -> rule.acceptCatalog(identity.getUser(), c)).collect(Collectors.toSet()));
    }

    /**
     * Check if identity is allowed to create the specified schema in a catalog.
     *
     * @param identity
     * @param schema
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanCreateSchema(Identity identity, CatalogSchemaName schema)
    {
        log.debug(DEFAULT_INFO, identity.getUser(), methodName());
        checkIsAdmin(identity.getUser(), notAllow(identity.getUser(), "create schema"));
    }

    /**
     * Check if identity is allowed to drop the specified schema in a catalog.
     *
     * @param identity
     * @param schema
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanDropSchema(Identity identity, CatalogSchemaName schema)
    {
        log.debug(DEFAULT_INFO, identity.getUser(), methodName());
        checkIsAdmin(identity.getUser(), notAllow(identity.getUser(), "drop schema"));
    }

    /**
     * Check if identity is allowed to rename the specified schema in a catalog.
     *
     * @param identity
     * @param schema
     * @param newSchemaName
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanRenameSchema(Identity identity, CatalogSchemaName schema, String newSchemaName)
    {
        log.debug(DEFAULT_INFO, identity.getUser(), methodName());
        checkIsAdmin(identity.getUser(), notAllow(identity.getUser(), "rename schema"));
    }

    /**
     * Check if identity is allowed to execute SHOW SCHEMAS in a catalog.
     * <p>
     * NOTE: This method is only present to give users an error message when listing is not allowed.
     * The {@link #filterSchemas} method must filter all results for unauthorized users,
     * since there are multiple ways to list schemas.
     *
     * @param identity
     * @param catalogName
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanShowSchemas(Identity identity, String catalogName)
    {
        log.debug(DEFAULT_INFO, identity.getUser(), methodName());
        if (!rule.acceptCatalog(identity.getUser(), catalogName)) {
            throw new AccessDeniedException(String.format("User [%s] is not allowed to access catalog [%s]",
                    identity.getUser(), catalogName));
        }
    }

    /**
     * Filter the list of schemas in a catalog to those visible to the identity.
     *
     * @param identity
     * @param catalogName
     * @param schemaNames
     */
    @Override
    public Set<String> filterSchemas(Identity identity, String catalogName, Set<String> schemaNames)
    {
        log.debug(DEFAULT_INFO, identity.getUser(), methodName());
        if (!rule.acceptCatalog(identity.getUser(), catalogName)) {
            return ImmutableSet.of();
        }
        return ImmutableSet.copyOf(
                schemaNames.stream().filter(s -> rule.acceptSchema(identity.getUser(), catalogName, s))
                        .collect(Collectors.toSet()));
    }

    /**
     * Check if identity is allowed to create the specified table in a catalog.
     *
     * @param identity
     * @param table
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanCreateTable(Identity identity, CatalogSchemaTableName table)
    {
        log.debug(DEFAULT_INFO, identity.getUser(), methodName());
        checkIsAdmin(identity.getUser(), notAllow(identity.getUser(), "create table"));
    }

    /**
     * Check if identity is allowed to drop the specified table in a catalog.
     *
     * @param identity
     * @param table
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanDropTable(Identity identity, CatalogSchemaTableName table)
    {
        log.debug(DEFAULT_INFO, identity.getUser(), methodName());
        checkIsAdmin(identity.getUser(), notAllow(identity.getUser(), "drop table"));
    }

    /**
     * Check if identity is allowed to rename the specified table in a catalog.
     *
     * @param identity
     * @param table
     * @param newTable
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanRenameTable(Identity identity, CatalogSchemaTableName table, CatalogSchemaTableName newTable)
    {
        log.debug(DEFAULT_INFO, identity.getUser(), methodName());
        checkIsAdmin(identity.getUser(), notAllow(identity.getUser(), "rename table"));
    }

    /**
     * Check if identity is allowed to show metadata of tables by executing SHOW TABLES, SHOW GRANTS etc. in a catalog.
     * <p>
     * NOTE: This method is only present to give users an error message when listing is not allowed.
     * The {@link #filterTables} method must filter all results for unauthorized users,
     * since there are multiple ways to list tables.
     *
     * @param identity
     * @param schema
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanShowTablesMetadata(Identity identity, CatalogSchemaName schema)
    {
        log.debug(DEFAULT_INFO, identity.getUser(), methodName());
        if (!rule.acceptSchema(identity.getUser(), schema.getCatalogName(), schema.getSchemaName())) {
            throw new AccessDeniedException(String.format("User [%s] is not allowed to show the table metadata in [%s.%s]",
                    identity.getUser(), schema.getCatalogName(), schema.getSchemaName()));
        }
    }

    /**
     * Filter the list of tables and views to those visible to the identity.
     *
     * @param identity
     * @param catalogName
     * @param tableNames
     */
    @Override
    public Set<SchemaTableName> filterTables(Identity identity, String catalogName, Set<SchemaTableName> tableNames)
    {
        log.debug(DEFAULT_INFO, identity.getUser(), methodName());
        if (!rule.acceptCatalog(identity.getUser(), catalogName)) {
            return ImmutableSet.of();
        }
        return ImmutableSet.copyOf(
                tableNames.stream()
                        .filter(t -> rule.accept(identity.getUser(), catalogName, t.getSchemaName(), t.getTableName()))
                        .collect(Collectors.toSet()));
    }

    /**
     * Check if identity is allowed to add columns to the specified table in a catalog.
     *
     * @param identity
     * @param table
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanAddColumn(Identity identity, CatalogSchemaTableName table)
    {
        log.debug(DEFAULT_INFO, identity.getUser(), methodName());
        checkIsAdmin(identity.getUser(), notAllow(identity.getUser(), "add column"));
    }

    /**
     * Check if identity is allowed to drop columns from the specified table in a catalog.
     *
     * @param identity
     * @param table
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanDropColumn(Identity identity, CatalogSchemaTableName table)
    {
        log.debug(DEFAULT_INFO, identity.getUser(), methodName());
        checkIsAdmin(identity.getUser(), notAllow(identity.getUser(), "drop column"));
    }

    /**
     * Check if identity is allowed to rename a column in the specified table in a catalog.
     *
     * @param identity
     * @param table
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanRenameColumn(Identity identity, CatalogSchemaTableName table)
    {
        log.debug(DEFAULT_INFO, identity.getUser(), methodName());
        checkIsAdmin(identity.getUser(), notAllow(identity.getUser(), "rename column"));
    }

    /**
     * Check if identity is allowed to select from the specified table in a catalog.
     *
     * @param identity
     * @param table
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanSelectFromTable(Identity identity, CatalogSchemaTableName table)
    {
        log.debug(DEFAULT_INFO, identity.getUser(), methodName());
        if (!rule.accept(identity.getUser(),
                table.getCatalogName(),
                table.getSchemaTableName().getSchemaName(),
                table.getSchemaTableName().getTableName())) {
            String message = String.format("User [%s] is not allowed to select from %s.%s.%s", identity.getUser(),
                    table.getCatalogName(), table.getSchemaTableName().getSchemaName(),
                    table.getSchemaTableName().getTableName());
            throw new AccessDeniedException(message);
        }
    }

    /**
     * Check if identity is allowed to insert into the specified table in a catalog.
     *
     * @param identity
     * @param table
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanInsertIntoTable(Identity identity, CatalogSchemaTableName table)
    {
        log.debug(DEFAULT_INFO, identity.getUser(), methodName());
        checkIsAdmin(identity.getUser(), notAllow(identity.getUser(), "insert into table"));
    }

    /**
     * Check if identity is allowed to delete from the specified table in a catalog.
     *
     * @param identity
     * @param table
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanDeleteFromTable(Identity identity, CatalogSchemaTableName table)
    {
        log.debug(DEFAULT_INFO, identity.getUser(), methodName());
        checkIsAdmin(identity.getUser(), notAllow(identity.getUser(), "delete from table"));
    }

    /**
     * Check if identity is allowed to create the specified view in a catalog.
     *
     * @param identity
     * @param view
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanCreateView(Identity identity, CatalogSchemaTableName view)
    {
        log.debug(DEFAULT_INFO, identity.getUser(), methodName());
        checkIsAdmin(identity.getUser(), notAllow(identity.getUser(), "create view"));
    }

    /**
     * Check if identity is allowed to drop the specified view in a catalog.
     *
     * @param identity
     * @param view
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanDropView(Identity identity, CatalogSchemaTableName view)
    {
        log.debug(DEFAULT_INFO, identity.getUser(), methodName());
        checkIsAdmin(identity.getUser(), notAllow(identity.getUser(), "drop table"));
    }

    /**
     * Check if identity is allowed to select from the specified view in a catalog.
     *
     * @param identity
     * @param view
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanSelectFromView(Identity identity, CatalogSchemaTableName view)
    {
        log.debug(DEFAULT_INFO, identity.getUser(), methodName());
        checkCanSelectFromTable(identity, view);
    }

    /**
     * Check if identity is allowed to create a view that selects from the specified table in a catalog.
     *
     * @param identity
     * @param table
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanCreateViewWithSelectFromTable(Identity identity, CatalogSchemaTableName table)
    {
        log.debug(DEFAULT_INFO, identity.getUser(), methodName());
        this.checkCanSelectFromTable(identity, table);
    }

    /**
     * Check if identity is allowed to create a view that selects from the specified view in a catalog.
     *
     * @param identity
     * @param view
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanCreateViewWithSelectFromView(Identity identity, CatalogSchemaTableName view)
    {
        log.debug(DEFAULT_INFO, identity.getUser(), methodName());
        this.checkCanSelectFromView(identity, view);
    }

    /**
     * Check if identity is allowed to set the specified property in a catalog.
     *
     * @param identity
     * @param catalogName
     * @param propertyName
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanSetCatalogSessionProperty(Identity identity, String catalogName, String propertyName)
    {
        log.debug(DEFAULT_INFO, identity.getUser(), methodName());
        //TODO
    }

    /**
     * Check if identity is allowed to grant the specified privilege to the grantee on the specified table.
     *
     * @param identity
     * @param privilege
     * @param table
     * @param grantee
     * @param withGrantOption
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanGrantTablePrivilege(Identity identity, Privilege privilege,
            CatalogSchemaTableName table, String grantee,
            boolean withGrantOption)
    {
        log.debug(DEFAULT_INFO, identity.getUser(), methodName());
        checkIsAdmin(identity.getUser(), notAllow(identity.getUser(), "grant table privilege"));
    }

    /**
     * Check if identity is allowed to revoke the specified privilege on the specified table from the revokee.
     *
     * @param identity
     * @param privilege
     * @param table
     * @param revokee
     * @param grantOptionFor
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanRevokeTablePrivilege(Identity identity, Privilege privilege,
            CatalogSchemaTableName table, String revokee,
            boolean grantOptionFor)
    {
        log.debug(DEFAULT_INFO, identity.getUser(), methodName());
        checkIsAdmin(identity.getUser(), notAllow(identity.getUser(), "revoke table privilege"));
    }
}
