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

public class OperatorStats
{
    public int pipelineId;
    public int operatorId;
    public String planNodeId;
    public String operatorType;
    public long totalDrivers;
    public long addInputCalls;
    public long addInputWallMillis;
    public long addInputCpuMillis;
    public long addInputUserMillis;
    public long inputDataSizeBytes;
    public long inputPositions;
    public double sumSquaredInputPositions;
    public long getOutputCalls;
    public long getOutputWallMillis;
    public long getOutputCpuMillis;
    public long getOutputUserMillis;
    public long outputDataSizeBytes;
    public long outputPositions;
    public long blockedWallMillis;
    public long finishCalls;
    public long finishWallMillis;
    public long finishCpuMillis;
    public long finishUserMillis;
    // public long memoryReservationBytes; // 0.188
    public long userMemoryReservationBytes;
    public long revocableMemoryReservationBytes;
    public long systemMemoryReservationBytes;
}
