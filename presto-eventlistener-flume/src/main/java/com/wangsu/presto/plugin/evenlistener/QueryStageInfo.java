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

public class QueryStageInfo
{
    public int stageId;
    public int completedTasks;
    public int completedDrivers;
    public long totalScheduledTimeMillis;
    public long totalCpuTimeMillis;
    public long totalUserTimeMillis;
    public long totalBlockedTimeMillis;
    public long processedInputDataSizeBytes;
    public long processedInputPositions;
    public long rawInputDataSizeBytes;
    public long rawInputPositions;

    public long peakMemoryReservationBytes;
    //- memory pool
    public double cumulativeMemory;

    public long outputDataSizeBytes;
    public long outputPositions;
}
