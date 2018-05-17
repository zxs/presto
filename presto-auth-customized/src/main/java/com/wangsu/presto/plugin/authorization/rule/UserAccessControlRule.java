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
package com.wangsu.presto.plugin.authorization.rule;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import io.airlift.log.Logger;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Created by sunwl on 2018/4/4.
 */
public class UserAccessControlRule
{
    private static final Logger log = Logger.get(UserAccessControlRule.class);
    private final String user;
    private final boolean acceptAll;
    private final Map<String, CatalogAccessControlRule> rules;
    private static final String DOT = ".";
    private static final String STAR = "*";
    private final boolean isAdmin;

    @JsonCreator
    public UserAccessControlRule(
            @JsonProperty("user") String user,
            @JsonProperty("privilege") List<String> privilege,
            @JsonProperty("is_admin") Optional<String> isAdmin)
    {
        this.user = requireNonNull(user);
        this.isAdmin = isAdmin.map(String::trim).map(s -> "true".equalsIgnoreCase(s)).orElse(false);
        this.rules = new HashMap<>();
        if (this.isAdmin) {
            acceptAll = true;
            log.info("User [%s] is admin!", user);
            return;
        }

        if (Objects.isNull(privilege)) {
            privilege = ImmutableList.of();
        }

        privilege = privilege.stream().filter(Objects::nonNull).map(String::trim).collect(Collectors.toList());
        acceptAll = privilege.stream().anyMatch(p -> p.startsWith(STAR));
        if (acceptAll) {
            return;
        }
        privilege.stream().forEach(p -> addCatalog(p));
    }

    private void addCatalog(String privilege)
    {
        List<String> tokens = Splitter.on(DOT).limit(2).trimResults().splitToList(privilege);
        String catalog = tokens.get(0);
        if (tokens.size() != 2 || catalog.isEmpty()) {
            log.error("Find invalid privilege [%s], skip it.", privilege);
            return;
        }
        CatalogAccessControlRule rule = rules.get(catalog);
        rule = rule != null ? rule : new CatalogAccessControlRule(catalog);
        rules.putIfAbsent(catalog, rule);
        rule.addSchema(tokens.get(1));
    }

    public boolean isAdmin()
    {
        return this.isAdmin;
    }

    public boolean accept(String catalog, String schema, String table)
    {
        return acceptAll || Optional.ofNullable(rules.get(catalog)).map(c -> c.accept(schema, table)).orElse(false);
    }

    public boolean acceptSchema(String catalog, String schema)
    {
        return acceptAll || Optional.ofNullable(rules.get(catalog)).map(c -> c.acceptSchema(schema)).orElse(false);
    }

    public boolean acceptCatalog(String catalog)
    {
        return acceptAll || rules.containsKey(catalog);
    }

    public String getUser()
    {
        return user;
    }

    class CatalogAccessControlRule
    {
        private final String catalog;
        private boolean acceptAll;
        private final Map<String, SchemaAccessControlRule> rules;

        public CatalogAccessControlRule(String catalog)
        {
            this.catalog = catalog;
            this.rules = new HashMap<>();
        }

        public void addSchema(String privilege)
        {
            List<String> tokens = Splitter.on(DOT).limit(2).trimResults().splitToList(privilege);
            String schema = tokens.get(0);
            if (tokens.size() != 2) {
                if (schema.startsWith(STAR)) {
                    acceptAll = true;
                }
                else {
                    log.error("Find invalid schema and table privilege [%s], table name is required.", privilege);
                }
                return;
            }

            SchemaAccessControlRule rule = rules.get(schema);
            rule = rule != null ? rule : new SchemaAccessControlRule(schema);
            rules.putIfAbsent(schema, rule);
            rule.addTable(tokens.get(1));
        }

        public boolean accept(String schema, String table)
        {
            return acceptAll || Optional.ofNullable(rules.get(schema)).map(s -> s.accept(table)).orElse(false);
        }

        public boolean acceptSchema(String schema)
        {
            return acceptAll || rules.containsKey(schema);
        }
    }

    class SchemaAccessControlRule
    {
        private final String schema;
        private final Set<String> accepts;
        private final Set<String> rejects;
        private final List<String> fuzzy;
        private boolean acceptAll;

        public SchemaAccessControlRule(String schema)
        {
            this.schema = schema;
            this.accepts = Sets.newConcurrentHashSet();
            this.rejects = Sets.newConcurrentHashSet();
            this.fuzzy = new LinkedList<>();
        }

        public void addTable(String privilege)
        {
            if (STAR.equals(privilege)) {
                acceptAll = true;
            }
            else if (privilege.contains(".")) {
                log.error("Find invalid table [%s], skip it", privilege);
            }
            else if (privilege.endsWith(STAR)) {
                fuzzy.add(privilege.replace(STAR, ""));
            }
            else {
                accepts.add(privilege);
            }
        }

        public boolean accept(String table)
        {
            if (acceptAll || accepts.contains(table)) {
                return true;
            }
            if (rejects.contains(table)) {
                return false;
            }
            if (fuzzy.stream().filter(table::startsWith).count() > 0) {
                accepts.add(table);
                return true;
            }
            else {
                rejects.add(table);
                return false;
            }
        }
    }
}
