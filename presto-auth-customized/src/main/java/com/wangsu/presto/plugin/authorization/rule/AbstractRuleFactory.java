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

import com.google.inject.Inject;
import io.airlift.log.Logger;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.List;
import java.util.Optional;

import static io.airlift.json.JsonCodec.jsonCodec;
import static java.util.Objects.requireNonNull;

public abstract class AbstractRuleFactory
        implements RuleFactory
{
    private static final Logger log = Logger.get(AbstractRuleFactory.class);
    private String digest;
    @Inject
    protected RuleConfig config;
    @Inject
    protected MsAlarm msAlarm;
    private final AccessControlRuleWrapper wrapper = new AccessControlRuleWrapper();

    protected Optional<AccessControlRule> parse(byte[] content)
            throws IOException
    {
        return Optional.ofNullable(content)
                .map(c -> {
                    log.info(new String(c));
                    return c;
                })
                .map(c -> jsonCodec(AccessControlRule.class).fromJson(c));
    }

    private String getDigest(byte[] bytes)
            throws NoSuchAlgorithmException
    {
        MessageDigest md5 = MessageDigest.getInstance("MD5");
        return Base64.getEncoder().encodeToString(md5.digest(bytes));
    }

    @Override
    public Optional<AccessControlRule> newInstance()
    {
        try {
            byte[] jsonBytes = getJsonBytes(config);
            String currentDigest = null;
            try {
                currentDigest = getDigest(jsonBytes);
                if (digest != null && digest.equals(currentDigest)) {
                    log.info("Config is not changed");
                    return Optional.empty();
                }
            }
            catch (NoSuchAlgorithmException e) {
                log.warn("No found: MD5");
                throw e;
            }
            wrapper.update(parse(jsonBytes)
                    .orElseThrow(() -> new RuntimeException("Fail to create new instance")));
            digest = currentDigest;
            log.info("Update config completes");
            return Optional.of(wrapper);
        }
        catch (Throwable e) {
            log.error(e);
            msAlarm.alert("Fail to read authorization config, please check.");
            return Optional.empty();
        }
    }

    protected abstract byte[] getJsonBytes(RuleConfig config)
            throws Throwable;

    class AccessControlRuleWrapper
            extends AccessControlRule
    {
        private volatile AccessControlRule delegate;

        public AccessControlRuleWrapper(Optional<List<UserAccessControlRule>> rules)
        {
            super(rules);
            throw new RuntimeException("Unsupported operation");
        }

        public AccessControlRuleWrapper()
        {
            super(Optional.empty());
        }

        public void update(AccessControlRule delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public boolean isAdmin(String user)
        {
            return delegate.isAdmin(user);
        }

        @Override
        public boolean accept(String user, String catalog, String schema, String table)
        {
            requireNonNull(delegate);
            return delegate.accept(user, catalog, schema, table);
        }

        @Override
        public boolean acceptSchema(String user, String catalog, String schema)
        {
            requireNonNull(delegate);
            return delegate.acceptSchema(user, catalog, schema);
        }

        @Override
        public boolean acceptCatalog(String user, String catalog)
        {
            requireNonNull(delegate);
            return delegate.acceptCatalog(user, catalog);
        }
    }
}
