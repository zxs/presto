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
package com.facebook.presto.raptor.storage;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MinDataSize;

import javax.annotation.Nullable;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class StorageManagerConfig
{
    private File dataDirectory;
    private File backupDirectory;
    private Duration shardRecoveryTimeout = new Duration(30, TimeUnit.SECONDS);
    private Duration missingShardDiscoveryInterval = new Duration(5, TimeUnit.MINUTES);
    private DataSize orcMaxMergeDistance = new DataSize(1, MEGABYTE);
    private int recoveryThreads = 10;
    private long rowsPerShard = 1_000_000;
    private DataSize maxBufferSize = new DataSize(256, MEGABYTE);

    @NotNull
    public File getDataDirectory()
    {
        return dataDirectory;
    }

    @Config("storage.data-directory")
    @ConfigDescription("Base directory to use for storing shard data")
    public StorageManagerConfig setDataDirectory(File dataDirectory)
    {
        this.dataDirectory = dataDirectory;
        return this;
    }

    @Nullable
    public File getBackupDirectory()
    {
        return backupDirectory;
    }

    @Config("storage.backup-directory")
    @ConfigDescription("Base directory to use for the backup copy of shard data")
    public StorageManagerConfig setBackupDirectory(File backupDirectory)
    {
        this.backupDirectory = backupDirectory;
        return this;
    }

    @NotNull
    public DataSize getOrcMaxMergeDistance()
    {
        return orcMaxMergeDistance;
    }

    @Config("storage.orc.max-merge-distance")
    public StorageManagerConfig setOrcMaxMergeDistance(DataSize orcMaxMergeDistance)
    {
        this.orcMaxMergeDistance = orcMaxMergeDistance;
        return this;
    }

    public Duration getShardRecoveryTimeout()
    {
        return shardRecoveryTimeout;
    }

    @Config("storage.shard-recovery-timeout")
    @ConfigDescription("Maximum time to wait for a shard to recover from backup while running a query")
    public StorageManagerConfig setShardRecoveryTimeout(Duration shardRecoveryTimeout)
    {
        this.shardRecoveryTimeout = shardRecoveryTimeout;
        return this;
    }

    public Duration getMissingShardDiscoveryInterval()
    {
        return missingShardDiscoveryInterval;
    }

    @Config("storage.missing-shard-discovery-interval")
    @ConfigDescription("How often to check the database and local file system missing shards")
    public StorageManagerConfig setMissingShardDiscoveryInterval(Duration missingShardDiscoveryInterval)
    {
        this.missingShardDiscoveryInterval = missingShardDiscoveryInterval;
        return this;
    }

    @Min(1)
    public int getRecoveryThreads()
    {
        return recoveryThreads;
    }

    @Config("storage.max-recovery-threads")
    @ConfigDescription("Maximum number of threads to use for recovery")
    public StorageManagerConfig setRecoveryThreads(int recoveryThreads)
    {
        this.recoveryThreads = recoveryThreads;
        return this;
    }

    @Min(1)
    @Max(1_000_000_000)
    public long getRowsPerShard()
    {
        return rowsPerShard;
    }

    @Config("storage.rows-per-shard")
    @ConfigDescription("Approximate maximum number of rows per shard")
    public StorageManagerConfig setRowsPerShard(long rowsPerShard)
    {
        this.rowsPerShard = rowsPerShard;
        return this;
    }

    @MinDataSize("1MB")
    public DataSize getMaxBufferSize()
    {
        return maxBufferSize;
    }

    @Config("storage.max-buffer-size")
    @ConfigDescription("Maximum data to buffer before flushing to disk")
    public StorageManagerConfig setMaxBufferSize(DataSize maxBufferSize)
    {
        this.maxBufferSize = maxBufferSize;
        return this;
    }
}
