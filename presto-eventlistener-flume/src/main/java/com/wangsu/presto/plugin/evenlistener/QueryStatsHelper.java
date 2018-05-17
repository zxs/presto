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

import com.facebook.presto.spi.eventlistener.QueryMetadata;
import com.facebook.presto.spi.eventlistener.QueryStatistics;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue.ValueType;

import java.io.StringReader;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class QueryStatsHelper
{
    private static final Logger log = Logger.get(QueryStatsHelper.class);

    private QueryStatsHelper()
    {
        throw new AssertionError();
    }

    private static long getBytesOrNegativeOne(String strVal)
    {
        try {
            return DataSize.valueOf(strVal).toBytes();
        }
        catch (IllegalArgumentException e) {
            log.warn(e,
                    String.format("Failed to parse io.airlift.units.DataSize '%s', returning -1", strVal));
            return -1;
        }
    }

    private static long getMillisOrNegativeOne(String strVal)
    {
        try {
            return Duration.valueOf(strVal).toMillis();
        }
        catch (IllegalArgumentException e) {
            log.warn(e,
                    String.format("Failed to parse io.airlift.units.Duration '%s', returning -1", strVal));
            return -1;
        }
    }

    private static QueryStageInfo getQueryStageInfo(int stageId, JsonObject stage)
    {
        QueryStageInfo stageInfo = new QueryStageInfo();

        stageInfo.stageId = stageId;
        try {
            JsonObject stageStats = stage.getJsonObject("stageStats");
            stageInfo.rawInputDataSizeBytes = getBytesOrNegativeOne(stageStats.getString("rawInputDataSize"));
            stageInfo.rawInputPositions = stageStats.getJsonNumber("rawInputPositions").longValue();
            stageInfo.processedInputDataSizeBytes = getBytesOrNegativeOne(stageStats.getString("processedInputDataSize"));
            stageInfo.processedInputPositions = stageStats.getJsonNumber("processedInputPositions").longValue();
            stageInfo.outputDataSizeBytes = getBytesOrNegativeOne(stageStats.getString("outputDataSize"));
            stageInfo.outputPositions = stageStats.getJsonNumber("outputPositions").longValue();
            stageInfo.completedTasks = stageStats.getInt("completedTasks");
            stageInfo.completedDrivers = stageStats.getInt("completedDrivers");
            //stageInfo.cumulativeMemory = stageStats.getJsonNumber("cumulativeMemory").doubleValue(); // 0.188
            //stageInfo.peakMemoryReservationBytes = getBytesOrNegativeOne(stageStats.getString("peakMemoryReservation")); // 0.188

            stageInfo.cumulativeUserMemory = stageStats.getJsonNumber("cumulativeUserMemory").doubleValue(); // 0.202
            stageInfo.peakUserMemoryReservationBytes = getBytesOrNegativeOne(stageStats.getString("peakUserMemoryReservation")); // 0.202
            stageInfo.totalScheduledTimeMillis = getMillisOrNegativeOne(stageStats.getString("totalScheduledTime"));
            stageInfo.totalCpuTimeMillis = getMillisOrNegativeOne(stageStats.getString("totalCpuTime"));
            stageInfo.totalUserTimeMillis = getMillisOrNegativeOne(stageStats.getString("totalUserTime"));
            stageInfo.totalBlockedTimeMillis = getMillisOrNegativeOne(stageStats.getString("totalBlockedTime"));
        }
        catch (Exception e) {
            log.error(e, String.format("Error retrieving stage stats for stage %d", stageId));
            return null;
        }

        return stageInfo;
    }

    private static OperatorStats getOperatorStat(String operatorSummaryStr)
    {
        try {
            JsonReader jsonReader = Json.createReader(new StringReader(operatorSummaryStr));
            return getOperatorStat(jsonReader.readObject());
        }
        catch (Exception e) {
            log.error(e, String.format("Error retrieving operator stats from string:\n%s\n", operatorSummaryStr));
        }

        return null;
    }

    private static OperatorStats getOperatorStat(JsonObject obj)
    {
        OperatorStats operatorStats = new OperatorStats();

        try {
            operatorStats.pipelineId = obj.getInt("pipelineId");
            operatorStats.operatorId = obj.getInt("operatorId");
            operatorStats.planNodeId = obj.getString("planNodeId");
            operatorStats.operatorType = obj.getString("operatorType");
            operatorStats.totalDrivers = obj.getJsonNumber("totalDrivers").longValue();
            operatorStats.addInputCalls = obj.getJsonNumber("addInputCalls").longValue();
            operatorStats.addInputWallMillis = getMillisOrNegativeOne(obj.getString("addInputWall"));
            operatorStats.addInputCpuMillis = getMillisOrNegativeOne(obj.getString("addInputCpu"));
            operatorStats.addInputUserMillis = getMillisOrNegativeOne(obj.getString("addInputUser"));
            operatorStats.inputDataSizeBytes = getBytesOrNegativeOne(obj.getString("inputDataSize"));
            operatorStats.inputPositions = obj.getJsonNumber("inputPositions").longValue();
            operatorStats.sumSquaredInputPositions = obj.getJsonNumber("sumSquaredInputPositions").doubleValue();
            operatorStats.getOutputCalls = obj.getJsonNumber("getOutputCalls").longValue();
            operatorStats.getOutputWallMillis = getMillisOrNegativeOne(obj.getString("getOutputWall"));
            operatorStats.getOutputCpuMillis = getMillisOrNegativeOne(obj.getString("getOutputCpu"));
            operatorStats.getOutputUserMillis = getMillisOrNegativeOne(obj.getString("getOutputUser"));
            operatorStats.outputDataSizeBytes = getBytesOrNegativeOne(obj.getString("outputDataSize"));
            operatorStats.outputPositions = obj.getJsonNumber("outputPositions").longValue();
            operatorStats.blockedWallMillis = getMillisOrNegativeOne(obj.getString("blockedWall"));
            operatorStats.finishCalls = obj.getJsonNumber("finishCalls").longValue();
            operatorStats.finishWallMillis = getMillisOrNegativeOne(obj.getString("finishWall"));
            operatorStats.finishCpuMillis = getMillisOrNegativeOne(obj.getString("finishCpu"));
            operatorStats.finishUserMillis = getMillisOrNegativeOne(obj.getString("finishUser"));
            // operatorStats.memoryReservationBytes = getBytesOrNegativeOne(obj.getString("memoryReservation")); // 0.188
            // 0.202 +
            operatorStats.userMemoryReservationBytes = getBytesOrNegativeOne(obj.getString("userMemoryReservation"));
            operatorStats.revocableMemoryReservationBytes = getBytesOrNegativeOne(obj.getString("revocableMemoryReservation"));
            // ?? memoryReservation = userMemoryReservation + revocableMemoryReservation 0.
            operatorStats.systemMemoryReservationBytes = getBytesOrNegativeOne(obj.getString("systemMemoryReservation"));
        }
        catch (Exception e) {
            log.error(e, String.format("Error retrieving operator stats from JsonObject:\n%s\n", obj.toString()));
            return null;
        }

        return operatorStats;
    }

    public static Map<Integer, QueryStageInfo> getQueryStages(QueryMetadata eventMetadata)
    {
        Map<Integer, QueryStageInfo> stages = new TreeMap<>();
        if (!eventMetadata.getPayload().isPresent()) {
            return stages;
        }

        String payload = eventMetadata.getPayload().get();
        Queue<JsonObject> stageJsonObjs = new LinkedList<JsonObject>();
        try {
            JsonReader jsonReader = Json.createReader(new StringReader(payload));
            stageJsonObjs.add(jsonReader.readObject());
        }
        catch (Exception e) {
            log.error(e,
                    String.format("getQueryStages - Unable to extract JsonObject out of following blob:\n%s\n", payload));
            return stages;
        }

        while (!stageJsonObjs.isEmpty()) {
            JsonObject cur = stageJsonObjs.poll();
            String stageIdStr = "Unknown";
            try {
                stageIdStr = cur.getString("stageId");
                int stageId = Integer.parseInt(stageIdStr.split("\\.")[1]);
                QueryStageInfo curStage = getQueryStageInfo(stageId, cur);
                if (curStage != null) {
                    stages.put(stageId, curStage);
                }
            }
            catch (Exception e) {
                log.error(e,
                        String.format("Failed to parse QueryStageInfo from JsonObject:\n%s\n", cur.toString()));
                return stages;
            }

            try {
                cur.getJsonArray("subStages")
                        .stream()
                        .filter(val -> val.getValueType() == ValueType.OBJECT)
                        .forEach(val -> stageJsonObjs.add((JsonObject) val));
            }
            catch (Exception e) {
                log.error(e,
                        String.format("Failed to get subStages for stage %s, treating as no subStages", stageIdStr));
            }
        }

        return stages;
    }

    public static List<OperatorStats> getOperatorSummaries(QueryStatistics eventStat)
    {
        try {
            return eventStat.getOperatorSummaries()
                    .stream()
                    .filter(val -> val != null && !val.isEmpty())
                    .map(QueryStatsHelper::getOperatorStat)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        }
        catch (Exception e) {
            log.error(e,
                    String.format("Error converting List<String> to List<OperatorStats>:\n%s\n", eventStat.getOperatorSummaries().toString()));
        }

        return null;
    }
}
