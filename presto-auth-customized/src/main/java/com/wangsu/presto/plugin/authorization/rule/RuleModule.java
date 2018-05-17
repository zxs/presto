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

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

/**
 * Created by sunwl on 2018/4/13.
 */
public class RuleModule
        extends AbstractConfigurationAwareModule
{
    private String name;

    public RuleModule(String name)
    {
        this.name = requireNonNull(name);
    }

    @Override
    protected void setup(Binder binder)
    {
        requireNonNull(name);
        configBinder(binder).bindConfig(RuleConfig.class);
        try {
            binder.bind(MsAlarm.class).in(Scopes.SINGLETON);
            Class clz = Class.forName(name);
            binder.bind(RuleFactory.class).to(clz).in(Scopes.SINGLETON);
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
