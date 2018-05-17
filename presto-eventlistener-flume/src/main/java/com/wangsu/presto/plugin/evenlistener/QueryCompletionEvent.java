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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import org.apache.flume.Event;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryCompletionEvent
        implements Event
{
    private static final ObjectMapper mapper = new ObjectMapper()
            .configure(JsonGenerator.Feature.WRITE_NUMBERS_AS_STRINGS, true);
    private Map<String, String> headers;
    private byte[] body;

    private QueryCompletionEvent()
    {
        headers = new HashMap<String, String>();
        body = new byte[0];
    }

    @Override
    public Map<String, String> getHeaders()
    {
        return headers;
    }

    @Override
    public void setHeaders(Map<String, String> headers)
    {
        this.headers = headers;
    }

    @Override
    public byte[] getBody()
    {
        return body;
    }

    @Override
    public void setBody(byte[] body)
    {
        if (body == null) {
            body = new byte[0];
        }
        this.body = body;
    }

    public static final class Builder
    {
        private String serverVersion;
        private String environment;
        //#Client Info
        private String clientInfo;

        //#Session
        private String queryId;
        private String transactionId;
        private QueryState queryState;

        private String user;
        private String principal;
        private String source;
        private String remoteClientAddress;
        private String userAgent;

        private long createTimestamp;
        private long endTimestamp;

        //#Data Source
        private String catalog;
        private String schema;
        private Map<String, List<String>> queriedColumnsByTable;

        //#Execution
        private long elapsedTimeMs;
        private long queuedTimeMs;
        //Resource Group

        private String uri;
        private String query;

        //#Resource Utilization Summary
        private long cpuTimeMs;
        private long scheduledTimeMs;
        private long blockedTimeMs;
        private long inputRows;
        private long inputBytes;
        private long rawInputRows;
        private long rawInputBytes;
        private long peakMemoryBytes;
        //Memory Pool
        private double cumulativeMemoryByteSecond;
        private long outputRows;
        private long outputBytes;
        private long writtenRows;
        private long writtenBytes;

        private long executionStartTimeMs;

        private long analysisTimeMs;
        private long distributedPlanningTimeMs;

        private Map<Integer, QueryStageInfo> queryStages;
        private List<OperatorStats> operatorSummaries;

        private int splits;

        private int errorCodeId;
        private String errorCodeName;
        private String failureType;
        private String failureMessage;
        private String failureTask;
        private String failureHost;
        private String failuresJson;

        public Builder() {}

        public Builder withServerVersion(String serverVersion)
        {
            this.serverVersion = serverVersion;
            return this;
        }

        public Builder withEnvironment(String environment)
        {
            this.environment = environment;
            return this;
        }

        public Builder withClientInfo(String clientInfo)
        {
            this.clientInfo = clientInfo;
            return this;
        }

        public Builder withQueryId(String queryId)
        {
            this.queryId = queryId;
            return this;
        }

        public Builder withTransactionId(String transactionId)
        {
            this.transactionId = transactionId;
            return this;
        }

        public Builder withUser(String user)
        {
            this.user = user;
            return this;
        }

        public Builder withPrincipal(String principal)
        {
            this.principal = principal;
            return this;
        }

        public Builder withSource(String source)
        {
            this.source = source;
            return this;
        }

        public Builder withCatalog(String catalog)
        {
            this.catalog = catalog;
            return this;
        }

        public Builder withSchema(String schema)
        {
            this.schema = schema;
            return this;
        }

        public Builder withQueriedColumnsByTable(Map<String, List<String>> queriedColumnsByTable)
        {
            this.queriedColumnsByTable = queriedColumnsByTable;
            return this;
        }

        public Builder withRemoteClientAddress(String remoteClientAddress)
        {
            this.remoteClientAddress = remoteClientAddress;
            return this;
        }

        public Builder withUserAgent(String userAgent)
        {
            this.userAgent = userAgent;
            return this;
        }

        public Builder withQueryState(QueryState queryState)
        {
            this.queryState = queryState;
            return this;
        }

        public Builder withUri(String uri)
        {
            this.uri = uri;
            return this;
        }

        public Builder withQuery(String query)
        {
            this.query = query;
            return this;
        }

        public Builder withCreateTimestamp(long createTimeMs)
        {
            this.createTimestamp = createTimeMs;
            return this;
        }

        public Builder withExecutionStartTimeMs(long executionStartTimeMs)
        {
            this.executionStartTimeMs = executionStartTimeMs;
            return this;
        }

        public Builder withEndTimestamp(long endTimeMs)
        {
            this.endTimestamp = endTimeMs;
            return this;
        }

        public Builder withElapsedTimeMs(long elapsedTimeMs)
        {
            this.elapsedTimeMs = elapsedTimeMs;
            return this;
        }

        public Builder withQueuedTimeMs(long queuedTimeMs)
        {
            this.queuedTimeMs = queuedTimeMs;
            return this;
        }

        public Builder withScheduledTimeMs(long queryWallTimeMs)
        {
            this.scheduledTimeMs = queryWallTimeMs;
            return this;
        }

        public Builder withCumulativeMemoryByteSecond(double cumulativeMemoryByteSecond)
        {
            this.cumulativeMemoryByteSecond = cumulativeMemoryByteSecond;
            return this;
        }

        public Builder withPeakMemoryBytes(long peakMemoryBytes)
        {
            this.peakMemoryBytes = peakMemoryBytes;
            return this;
        }

        public Builder withCpuTimeMs(long cpuTimeMs)
        {
            this.cpuTimeMs = cpuTimeMs;
            return this;
        }

        public Builder withBlockedTimeMs(long blockedTimeMs)
        {
            this.blockedTimeMs = blockedTimeMs;
            return this;
        }

        public Builder withAnalysisTimeMs(long analysisTimeMs)
        {
            this.analysisTimeMs = analysisTimeMs;
            return this;
        }

        public Builder withDistributedPlanningTimeMs(long distributedPlanningTimeMs)
        {
            this.distributedPlanningTimeMs = distributedPlanningTimeMs;
            return this;
        }

        public Builder withInputRows(long inputRows)
        {
            this.inputRows = inputRows;
            return this;
        }

        public Builder withInputBytes(long inputBytes)
        {
            this.inputBytes = inputBytes;
            return this;
        }

        public Builder withRawInputRows(long rawInputRows)
        {
            this.rawInputRows = rawInputRows;
            return this;
        }

        public Builder withRawInputBytes(long rawInputBytes)
        {
            this.rawInputBytes = rawInputBytes;
            return this;
        }

        public Builder withOutputRows(long outputRows)
        {
            this.outputRows = outputRows;
            return this;
        }

        public Builder withOutputBytes(long outputBytes)
        {
            this.outputBytes = outputBytes;
            return this;
        }

        public Builder withWrittenRows(long writtenRows)
        {
            this.writtenRows = writtenRows;
            return this;
        }

        public Builder withWrittenBytes(long writtenBytes)
        {
            this.writtenBytes = writtenBytes;
            return this;
        }

        public Builder withQueryStages(Map<Integer, QueryStageInfo> queryStages)
        {
            this.queryStages = queryStages;
            return this;
        }

        public Builder withOperatorSummaries(List<OperatorStats> operatorSummaries)
        {
            this.operatorSummaries = operatorSummaries;
            return this;
        }

        public Builder withSplits(int splits)
        {
            this.splits = splits;
            return this;
        }

        public Builder withErrorCodeId(int errorCodeId)
        {
            this.errorCodeId = errorCodeId;
            return this;
        }

        public Builder withErrorCodeName(String errorCodeName)
        {
            this.errorCodeName = errorCodeName;
            return this;
        }

        public Builder withFailureType(String failureType)
        {
            this.failureType = failureType;
            return this;
        }

        public Builder withFailureMessage(String failureMessage)
        {
            this.failureMessage = failureMessage;
            return this;
        }

        public Builder withFailureTask(String failureTask)
        {
            this.failureTask = failureTask;
            return this;
        }

        public Builder withFailureHost(String failureHost)
        {
            this.failureHost = failureHost;
            return this;
        }

        public Builder withFailuresJson(String failuresJson)
        {
            this.failuresJson = failuresJson;
            return this;
        }

        public QueryCompletionEvent build()
        {
            QueryCompletionEvent queryCompletionEvent = new QueryCompletionEvent();
            Instant createTime = Instant.ofEpochMilli(createTimestamp);
            OffsetDateTime zDatetime = createTime.atOffset(ZoneOffset.ofHours(+8));
            Map<String, String> headers = new HashMap<String, String>();

            headers.put("y", String.valueOf(zDatetime.getYear()));
            headers.put("m", String.valueOf(zDatetime.getMonthValue()));
            headers.put("d", String.valueOf(zDatetime.getDayOfMonth()));
            headers.put("h", String.valueOf(zDatetime.getHour()));
            queryCompletionEvent.setHeaders(headers);

            try {
                byte[] body = Joiner.on("\t").useForNull("").join(
                        serverVersion,
                        environment,
                        //#Client Info
                        clientInfo,
                        //#Session
                        queryId,
                        transactionId,
                        queryState,

                        user,
                        principal,
                        source,
                        remoteClientAddress,
                        userAgent,

                        createTimestamp,
                        endTimestamp,

                        //#Data Source
                        catalog,
                        schema,
                        queriedColumnsByTable,

                        //#Execution
                        elapsedTimeMs,
                        queuedTimeMs,
                        //Resource Group

                        uri,
                        query,

                        //#Resource Utilization Summary
                        cpuTimeMs,
                        scheduledTimeMs,
                        blockedTimeMs,
                        inputRows,
                        inputBytes,
                        rawInputRows,
                        rawInputBytes,
                        peakMemoryBytes,
                        //Memory Pool
                        cumulativeMemoryByteSecond,
                        outputRows,
                        outputBytes,
                        writtenRows,
                        writtenBytes,

                        executionStartTimeMs,

                        analysisTimeMs,
                        distributedPlanningTimeMs,

                        mapper.writeValueAsString(queryStages),
                        mapper.writeValueAsString(operatorSummaries),

                        splits,

                        errorCodeId,
                        errorCodeName,
                        failureType,
                        failureMessage,
                        failureTask,
                        failureHost,
                        failuresJson
                ).getBytes(StandardCharsets.UTF_8);
                queryCompletionEvent.setBody(body);
            }
            catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            return queryCompletionEvent;
        }
    }
}
