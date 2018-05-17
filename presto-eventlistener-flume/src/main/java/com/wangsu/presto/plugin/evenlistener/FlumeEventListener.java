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
package com.wangsu.presto.plugin.evenlistener;

import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import com.facebook.presto.spi.eventlistener.SplitCompletedEvent;
import io.airlift.log.Logger;

import java.util.Map;

public class FlumeEventListener
        implements EventListener
{
    private static final Logger log = Logger.get(FlumeEventListener.class);
    private final FlumeClient flumeClient;

    public FlumeEventListener(Map<String, String> config)
    {
        flumeClient = new FlumeClient(config);
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
        log.debug("queryCreated: %s", queryCreatedEvent);
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        log.debug("queryCompleted %s", queryCompletedEvent);
        flumeClient.send(queryCompletedEvent);
    }

    @Override
    public void splitCompleted(SplitCompletedEvent splitCompletedEvent)
    {
    }
}
