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

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import javax.validation.constraints.NotNull;

import java.io.File;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestStorageManagerConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(StorageManagerConfig.class)
                .setDataDirectory(null)
                .setBackupDirectory(null)
                .setOrcMaxMergeDistance(new DataSize(1, MEGABYTE))
                .setShardRecoveryTimeout(new Duration(30, SECONDS))
                .setMissingShardDiscoveryInterval(new Duration(5, MINUTES))
                .setRecoveryThreads(10)
                .setRowsPerShard(1_000_000)
                .setMaxBufferSize(new DataSize(256, MEGABYTE)));

    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("storage.data-directory", "/data")
                .put("storage.backup-directory", "/backup")
                .put("storage.orc.max-merge-distance", "16kB")
                .put("storage.shard-recovery-timeout", "1m")
                .put("storage.missing-shard-discovery-interval", "4m")
                .put("storage.max-recovery-threads", "12")
                .put("storage.rows-per-shard", "10000")
                .put("storage.max-buffer-size", "512MB")
                .build();

        StorageManagerConfig expected = new StorageManagerConfig()
                .setDataDirectory(new File("/data"))
                .setBackupDirectory(new File("/backup"))
                .setOrcMaxMergeDistance(new DataSize(16, KILOBYTE))
                .setShardRecoveryTimeout(new Duration(1, MINUTES))
                .setMissingShardDiscoveryInterval(new Duration(4, MINUTES))
                .setRecoveryThreads(12)
                .setRowsPerShard(10_000)
                .setMaxBufferSize(new DataSize(512, MEGABYTE));

        assertFullMapping(properties, expected);
    }

    @Test
    public void testValidations()
    {
        assertFailsValidation(new StorageManagerConfig().setDataDirectory(null), "dataDirectory", "may not be null", NotNull.class);
    }
}
