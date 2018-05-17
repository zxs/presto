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
import com.facebook.presto.spi.security.SystemAccessControlFactory;
import com.google.inject.Injector;
import com.wangsu.presto.plugin.authorization.rule.AccessControlRule;
import com.wangsu.presto.plugin.authorization.rule.RuleFactory;
import com.wangsu.presto.plugin.authorization.rule.RuleManager;
import com.wangsu.presto.plugin.authorization.rule.RuleModule;
import io.airlift.bootstrap.Bootstrap;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Created by sunwl on 2018/4/4.
 */
public class CustomizedSystemAccessControlFactory
        implements SystemAccessControlFactory
{
    private static final String NAME = "customized-auth";
    private static final String PROVIDER_NAME = "security.config-provider";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public SystemAccessControl create(Map<String, String> config)
    {
        Bootstrap app = new Bootstrap(new RuleModule(requireNonNull(config.get(PROVIDER_NAME))));
        try {
            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();
            RuleFactory factory = injector.getInstance(RuleFactory.class);
            RuleManager manager = factory.getManager().orElseThrow(() -> new RuntimeException("Fail to get rule manager"));
            AccessControlRule rule = factory.newInstance().orElseThrow(() -> new RuntimeException("Fail to get rule"));

            SystemAccessControl control = new CustomizedSystemAccessControl(rule);
            manager.start();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> manager.stop()));
            return control;
        }
        catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }
}
