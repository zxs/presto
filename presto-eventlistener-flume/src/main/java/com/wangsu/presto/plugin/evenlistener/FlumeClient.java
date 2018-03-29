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

import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryContext;
import com.facebook.presto.spi.eventlistener.QueryFailureInfo;
import com.facebook.presto.spi.eventlistener.QueryMetadata;
import com.facebook.presto.spi.eventlistener.QueryStatistics;
import io.airlift.log.Logger;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class FlumeClient
{
    private static final Logger log = Logger.get(FlumeClient.class);
    private static final String DASH = "-";
    private static final String PREFIX_FLUME = "flume.";
    private final Properties flumeConfig = new Properties();
    private int maxRetries;
    private RpcClient client;

    public FlumeClient(Map<String, String> config)
    {
        log.info("event listener config %s", config);
        maxRetries = Integer.getInteger(config.get("event.send.maxRetries"), 1);
        maxRetries = maxRetries < 0 ? 0 : maxRetries;
        flumeConfig.putAll(config.entrySet()
                .stream()
                .filter(e -> e.getKey().startsWith(PREFIX_FLUME))
                .collect(Collectors.toMap(e -> e.getKey().substring(PREFIX_FLUME.length()), e -> e.getValue())));

        log.info("flume client config \n %s", flumeConfig);
        try {
            client = RpcClientFactory.getInstance(flumeConfig);
        }
        catch (FlumeException fe) {
            log.warn(fe, "Flume Client is NOT instantiated by %s, Please Check Flume Server/Client Config!", flumeConfig);
        }
    }

    public void send(QueryCompletedEvent event)
    {
        int retries = 0;
        while (retries <= maxRetries) {
            try {
                if (client == null) {
                    throw new EventDeliveryException("Flume Client is NOT initialized");
                }
                client.append(toFlumeQueryCompletionEvent(event));
                return; //
            }
            catch (EventDeliveryException ede) {
                retries++;
                log.warn(ede,
                        "#%d Could not send object of Query(id=%s, user=%s, env=%s, schema=%s.%s)",
                        retries,
                        event.getMetadata().getQueryId(),
                        event.getContext().getUser(),
                        event.getContext().getEnvironment(),
                        event.getContext().getCatalog().orElse(DASH),
                        event.getContext().getSchema().orElse(DASH));
                try {
                    if (client != null) {
                        client.close();
                    }
                    client = null;
                    client = RpcClientFactory.getInstance(flumeConfig);
                }
                catch (FlumeException fe) {
                    log.warn(fe, "Flume Client is NOT re-instantiated by %s", flumeConfig);
                }
            }
        }
    }

    QueryCompletionEvent toFlumeQueryCompletionEvent(QueryCompletedEvent event)
    {
        QueryMetadata eventMetadata = event.getMetadata();
        QueryContext eventContext = event.getContext();
        QueryStatistics eventStat = event.getStatistics();

        QueryCompletionEvent.Builder builder = new QueryCompletionEvent.Builder();

        builder.withServerVersion(eventContext.getServerVersion());
        builder.withEnvironment(eventContext.getEnvironment());
        // Client Info
        builder.withClientInfo(eventContext.getClientInfo().orElse(DASH));
        // Session
        builder.withQueryId(eventMetadata.getQueryId());
        builder.withTransactionId(eventMetadata.getTransactionId().orElse(DASH));
        builder.withQueryState(QueryState.valueOf(eventMetadata.getQueryState()));

        builder.withUser(eventContext.getUser());
        builder.withPrincipal(eventContext.getPrincipal().orElse(DASH));
        builder.withSource(eventContext.getSource().orElse(DASH));
        builder.withRemoteClientAddress(eventContext.getRemoteClientAddress().orElse(DASH));
        builder.withUserAgent(eventContext.getUserAgent().orElse(DASH));

        builder.withCreateTimestamp(event.getCreateTime().toEpochMilli());
        builder.withEndTimestamp(event.getEndTime().toEpochMilli());

        // Data Source
        builder.withCatalog(eventContext.getCatalog().orElse(DASH));
        builder.withSchema(eventContext.getSchema().orElse(DASH));
        Map<String, List<String>> queriedColumnsByTable = new HashMap<String, List<String>>();
        event.getIoMetadata().getInputs().forEach(input -> queriedColumnsByTable.put(String.format("%s.%s", input.getSchema(), input.getTable()), input.getColumns()));
        builder.withQueriedColumnsByTable(queriedColumnsByTable);

        // Execution
        builder.withElapsedTimeMs(event.getEndTime().toEpochMilli() - event.getCreateTime().toEpochMilli());
        builder.withQueuedTimeMs(eventStat.getQueuedTime().toMillis());

        builder.withUri(eventMetadata.getUri().toString());
        builder.withQuery(eventMetadata.getQuery());

        //Resource Utilization Summary
        builder.withCpuTimeMs(eventStat.getCpuTime().toMillis());
        builder.withScheduledTimeMs(eventStat.getWallTime().toMillis());
        Map<Integer, QueryStageInfo> queryStages = QueryStatsHelper.getQueryStages(eventMetadata);
        builder.withBlockedTimeMs(queryStages.values().stream().mapToLong(v -> v.totalBlockedTimeMillis).sum());
        // input rows ?
        builder.withInputRows(queryStages.values().stream().mapToLong(v -> v.processedInputPositions).sum());
        // input data ?
        builder.withInputBytes(queryStages.values().stream().mapToLong(v -> v.processedInputDataSizeBytes).sum());
        builder.withRawInputRows(eventStat.getTotalRows());
        builder.withRawInputBytes(eventStat.getTotalBytes());

        builder.withPeakMemoryBytes(eventStat.getPeakMemoryBytes());
        builder.withCumulativeMemoryByteSecond(eventStat.getCumulativeMemory());

        builder.withOutputRows(eventStat.getOutputRows());
        builder.withOutputBytes(eventStat.getOutputBytes());
        builder.withWrittenRows(eventStat.getWrittenRows());
        builder.withWrittenBytes(eventStat.getWrittenBytes());

        builder.withExecutionStartTimeMs(event.getExecutionStartTime().toEpochMilli());

        if (eventStat.getAnalysisTime().isPresent()) {
            builder.withAnalysisTimeMs(eventStat.getAnalysisTime().get().toMillis());
        }
        if (eventStat.getDistributedPlanningTime().isPresent()) {
            builder.withDistributedPlanningTimeMs(eventStat.getDistributedPlanningTime().get().toMillis());
        }

        builder.withQueryStages(queryStages);
        builder.withOperatorSummaries(QueryStatsHelper.getOperatorSummaries(eventStat));

        builder.withSplits(eventStat.getCompletedSplits());

        if (event.getFailureInfo().isPresent()) {
            QueryFailureInfo eventFailureInfo = event.getFailureInfo().get();
            builder.withErrorCodeId(eventFailureInfo.getErrorCode().getCode());
            builder.withErrorCodeName(eventFailureInfo.getErrorCode().getName());
            builder.withFailureType(eventFailureInfo.getFailureType().orElse(DASH));
            builder.withFailureMessage(eventFailureInfo.getFailureMessage().orElse(DASH));
            builder.withFailureTask(eventFailureInfo.getFailureTask().orElse(DASH));
            builder.withFailureHost(eventFailureInfo.getFailureHost().orElse(DASH));
            builder.withFailuresJson(eventFailureInfo.getFailuresJson());
        }

        return builder.build();
    }
}
