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
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Created by sunwl on 2018/4/4.
 */
public class AccessControlRule
{
    private static final Logger log = Logger.get(AccessControlRule.class);
    private final Map<String, UserAccessControlRule> rules;
    private static final String DEFAULT = "_default_";
    private final UserAccessControlRule defaultRule;

    @JsonCreator
    public AccessControlRule(@JsonProperty("users") Optional<List<UserAccessControlRule>> rules)
    {
        this.rules = rules.map(ImmutableList::copyOf).orElse(ImmutableList.of())
                .stream().collect(Collectors.toMap(UserAccessControlRule::getUser, r -> r));
        this.defaultRule = this.rules.remove(DEFAULT);
    }

    public boolean isAdmin(String user)
    {
        return Optional.ofNullable(rules.get(user)).map(UserAccessControlRule::isAdmin).orElse(false)
                || Optional.ofNullable(defaultRule).map(UserAccessControlRule::isAdmin).orElse(false);
    }

    public boolean accept(String user, String catalog, String schema, String table)
    {
        return Optional.ofNullable(rules.get(user)).map(u -> u.accept(catalog, schema, table)).orElse(false)
                || Optional.ofNullable(defaultRule).map(u -> u.accept(catalog, schema, table)).orElse(false);
    }

    public boolean acceptSchema(String user, String catalog, String schema)
    {
        return Optional.ofNullable(rules.get(user)).map(u -> u.acceptSchema(catalog, schema)).orElse(false)
                || Optional.ofNullable(defaultRule).map(u -> u.acceptSchema(catalog, schema)).orElse(false);
    }

    public boolean acceptCatalog(String user, String catalog)
    {
        return Optional.ofNullable(rules.get(user)).map(u -> u.acceptCatalog(catalog)).orElse(false)
                || Optional.ofNullable(defaultRule).map(u -> u.acceptCatalog(catalog)).orElse(false);
    }
}
