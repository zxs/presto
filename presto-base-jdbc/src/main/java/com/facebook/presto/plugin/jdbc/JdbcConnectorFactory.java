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
package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.classloader.ClassLoaderSafeConnectorMetadata;
import com.facebook.presto.spi.classloader.ClassLoaderSafeConnectorRecordSetProvider;
import com.facebook.presto.spi.classloader.ClassLoaderSafeConnectorRecordSinkProvider;
import com.facebook.presto.spi.classloader.ClassLoaderSafeConnectorSplitManager;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public class JdbcConnectorFactory
        implements ConnectorFactory
{
    private final String name;
    private final Module module;
    private final Map<String, String> optionalConfig;
    private final ClassLoader classLoader;

    public JdbcConnectorFactory(String name, Module module, Map<String, String> optionalConfig, ClassLoader classLoader)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or empty");
        this.name = name;
        this.module = requireNonNull(module, "module is null");
        this.optionalConfig = ImmutableMap.copyOf(requireNonNull(optionalConfig, "optionalConfig is null"));
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new JdbcHandleResolver();
    }

    @Override
    public Connector create(String connectorId, Map<String, String> requiredConfig)
    {
        requireNonNull(requiredConfig, "requiredConfig is null");
        requireNonNull(optionalConfig, "optionalConfig is null");

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(new JdbcModule(connectorId), module);

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(requiredConfig)
                    .setOptionalConfigurationProperties(optionalConfig)
                    .initialize();

            LifeCycleManager lifeCycleManager = injector.getInstance(LifeCycleManager.class);
            ConnectorMetadata connectorMetadata = injector.getInstance(ConnectorMetadata.class);
            ConnectorSplitManager connectorSplitManager = injector.getInstance(ConnectorSplitManager.class);
            ConnectorRecordSetProvider connectorRecordSetProvider = injector.getInstance(ConnectorRecordSetProvider.class);
            ConnectorHandleResolver connectorHandleResolver = injector.getInstance(ConnectorHandleResolver.class);
            ConnectorRecordSinkProvider connectorRecordSinkProvider = injector.getInstance(ConnectorRecordSinkProvider.class);

            return new JdbcConnector(
                lifeCycleManager,
                new ClassLoaderSafeConnectorMetadata(connectorMetadata, classLoader),
                new ClassLoaderSafeConnectorSplitManager(connectorSplitManager, classLoader),
                new ClassLoaderSafeConnectorRecordSetProvider(connectorRecordSetProvider, classLoader),
                new ClassLoaderSafeConnectorRecordSinkProvider(connectorRecordSinkProvider, classLoader));

            //return injector.getInstance(JdbcConnector.class);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
