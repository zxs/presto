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
package com.facebook.presto.raptor;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorIndexResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorPageSinkProvider;
import com.facebook.presto.spi.ConnectorPageSourceProvider;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.ConnectorSplitManager;

import javax.inject.Inject;

import static com.google.common.base.Preconditions.checkNotNull;

public class RaptorConnector
        implements Connector
{
    private final RaptorMetadata metadata;
    private final RaptorSplitManager splitManager;
    private final RaptorPageSourceProvider pageSourceProvider;
    private final RaptorPageSinkProvider pageSinkProvider;
    private final RaptorHandleResolver handleResolver;

    @Inject
    public RaptorConnector(
            RaptorMetadata metadata,
            RaptorSplitManager splitManager,
            RaptorPageSourceProvider pageSourceProvider,
            RaptorPageSinkProvider pageSinkProvider,
            RaptorHandleResolver handleResolver)
    {
        this.metadata = checkNotNull(metadata, "metadata is null");
        this.splitManager = checkNotNull(splitManager, "splitManager is null");
        this.pageSourceProvider = checkNotNull(pageSourceProvider, "pageSourceProvider is null");
        this.pageSinkProvider = checkNotNull(pageSinkProvider, "pageSinkProvider is null");
        this.handleResolver = checkNotNull(handleResolver, "handleResolver is null");
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return pageSinkProvider;
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return handleResolver;
    }

    @Override
    public ConnectorMetadata getMetadata()
    {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorRecordSinkProvider getRecordSinkProvider()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConnectorIndexResolver getIndexResolver()
    {
        throw new UnsupportedOperationException();
    }
}
