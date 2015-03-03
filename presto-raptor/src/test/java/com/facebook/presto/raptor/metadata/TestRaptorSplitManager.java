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
package com.facebook.presto.raptor.metadata;

import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.MetadataUtil.TableMetadataBuilder;
import com.facebook.presto.metadata.NodeVersion;
import com.facebook.presto.metadata.PrestoNode;
import com.facebook.presto.raptor.RaptorColumnHandle;
import com.facebook.presto.raptor.RaptorConnectorId;
import com.facebook.presto.raptor.RaptorMetadata;
import com.facebook.presto.raptor.RaptorSplitManager;
import com.facebook.presto.raptor.RaptorTableHandle;
import com.facebook.presto.raptor.storage.FileStorageService;
import com.facebook.presto.raptor.storage.ShardRecoveryManager;
import com.facebook.presto.raptor.storage.StorageManager;
import com.facebook.presto.raptor.storage.StorageService;
import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import io.airlift.json.JsonCodec;
import io.airlift.testing.FileUtils;
import io.airlift.units.Duration;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.raptor.metadata.TestDatabaseShardManager.shardInfo;
import static com.facebook.presto.raptor.storage.TestOrcStorageManager.createOrcStorageManager;
import static com.facebook.presto.raptor.util.Types.checkType;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestRaptorSplitManager
{
    private static final JsonCodec<ShardInfo> SHARD_INFO_CODEC = jsonCodec(ShardInfo.class);
    private static final ConnectorSession SESSION = new ConnectorSession("user", UTC_KEY, ENGLISH, System.currentTimeMillis(), null);
    private static final ConnectorTableMetadata TEST_TABLE = TableMetadataBuilder.tableMetadataBuilder("demo", "test_table")
            .partitionKeyColumn("ds", VARCHAR)
            .column("foo", VARCHAR)
            .column("bar", BigintType.BIGINT)
            .build();

    private Handle dummyHandle;
    private File dataDir;
    private RaptorSplitManager raptorSplitManager;
    private ConnectorTableHandle tableHandle;
    private ShardManager shardManager;
    private StorageManager storageManagerWithBackup;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        TypeRegistry typeRegistry = new TypeRegistry();
        DBI dbi = new DBI("jdbc:h2:mem:test" + System.nanoTime());
        dbi.registerMapper(new TableColumn.Mapper(typeRegistry));
        dummyHandle = dbi.open();
        dataDir = Files.createTempDir();
        shardManager = new DatabaseShardManager(dbi);
        InMemoryNodeManager nodeManager = new InMemoryNodeManager();

        StorageService storageService = new FileStorageService(dataDir, Optional.empty());
        StorageService storageServiceWithBackup = new FileStorageService(dataDir, Optional.of(Files.createTempDir()));
        ShardRecoveryManager recoveryManager = new ShardRecoveryManager(storageServiceWithBackup, new InMemoryNodeManager(), shardManager, new Duration(5, TimeUnit.MINUTES), 10);
        StorageManager storageManager = createOrcStorageManager(storageService, recoveryManager);
        storageManagerWithBackup = createOrcStorageManager(storageServiceWithBackup, recoveryManager);

        String nodeName = UUID.randomUUID().toString();
        nodeManager.addNode("raptor", new PrestoNode(nodeName, new URI("http://127.0.0.1/"), NodeVersion.UNKNOWN));

        RaptorConnectorId connectorId = new RaptorConnectorId("raptor");
        RaptorMetadata metadata = new RaptorMetadata(connectorId, dbi, shardManager, SHARD_INFO_CODEC);

        tableHandle = metadata.createTable(SESSION, TEST_TABLE);

        List<ShardInfo> shards = ImmutableList.<ShardInfo>builder()
                .add(shardInfo(UUID.randomUUID(), nodeName))
                .add(shardInfo(UUID.randomUUID(), nodeName))
                .add(shardInfo(UUID.randomUUID(), nodeName))
                .add(shardInfo(UUID.randomUUID(), nodeName))
                .build();

        long tableId = checkType(tableHandle, RaptorTableHandle.class, "tableHandle").getTableId();

        List<ColumnInfo> columns = metadata.getColumnHandles(tableHandle).values().stream()
                .map(handle -> checkType(handle, RaptorColumnHandle.class, "columnHandle"))
                .map(ColumnInfo::fromHandle)
                .collect(toList());

        shardManager.createTable(tableId, columns);
        shardManager.commitShards(tableId, columns, shards, Optional.empty());

        raptorSplitManager = new RaptorSplitManager(connectorId, nodeManager, shardManager, storageManager);
    }

    @AfterMethod
    public void teardown()
    {
        dummyHandle.close();
        FileUtils.deleteRecursively(dataDir);
    }

    @Test
    public void testSanity()
            throws InterruptedException
    {
        ConnectorPartitionResult partitionResult = raptorSplitManager.getPartitions(tableHandle, TupleDomain.<ConnectorColumnHandle>all());
        assertEquals(partitionResult.getPartitions().size(), 1);
        assertTrue(partitionResult.getUndeterminedTupleDomain().isAll());

        List<ConnectorPartition> partitions = partitionResult.getPartitions();
        ConnectorPartition partition = Iterables.getOnlyElement(partitions);
        TupleDomain<ConnectorColumnHandle> columnUnionedTupleDomain = TupleDomain.columnWiseUnion(partition.getTupleDomain(), partition.getTupleDomain());
        assertEquals(columnUnionedTupleDomain, TupleDomain.<ConnectorColumnHandle>all());

        ConnectorSplitSource splitSource = raptorSplitManager.getPartitionSplits(tableHandle, partitions);
        int splitCount = 0;
        while (!splitSource.isFinished()) {
            splitCount += splitSource.getNextBatch(1000).size();
        }
        assertEquals(splitCount, 4);
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "No host for shard .* found: \\[\\]")
    public void testNoHostForShard()
            throws InterruptedException
    {
        dummyHandle.execute("DELETE FROM shard_nodes");

        ConnectorPartitionResult result = raptorSplitManager.getPartitions(tableHandle, TupleDomain.<ConnectorColumnHandle>all());

        raptorSplitManager.getPartitionSplits(tableHandle, result.getPartitions()).getNextBatch(1000);
    }

    @Test
    public void testAssignRandomNodeWhenBackupAvailable()
            throws InterruptedException, URISyntaxException
    {
        InMemoryNodeManager nodeManager = new InMemoryNodeManager();
        PrestoNode node = new PrestoNode(UUID.randomUUID().toString(), new URI("http://127.0.0.1/"), NodeVersion.UNKNOWN);
        nodeManager.addNode("fbraptor", node);
        RaptorSplitManager raptorSplitManagerWithBackup = new RaptorSplitManager(new RaptorConnectorId("fbraptor"), nodeManager, shardManager, storageManagerWithBackup);

        dummyHandle.execute("DELETE FROM shard_nodes");

        ConnectorPartitionResult result = raptorSplitManagerWithBackup.getPartitions(tableHandle, TupleDomain.<ConnectorColumnHandle>all());
        ConnectorSplitSource partitionSplit = raptorSplitManagerWithBackup.getPartitionSplits(tableHandle, result.getPartitions());
        assertEquals(Iterables.getOnlyElement(Iterables.getOnlyElement(partitionSplit.getNextBatch(1)).getAddresses()), node.getHostAndPort());
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "No nodes available to run query")
    public void testNoNodes()
            throws InterruptedException, URISyntaxException
    {
        RaptorSplitManager raptorSplitManagerWithBackup = new RaptorSplitManager(new RaptorConnectorId("fbraptor"), new InMemoryNodeManager(), shardManager, storageManagerWithBackup);
        dummyHandle.execute("DELETE FROM shard_nodes");
        ConnectorPartitionResult result = raptorSplitManagerWithBackup.getPartitions(tableHandle, TupleDomain.<ConnectorColumnHandle>all());
        raptorSplitManagerWithBackup.getPartitionSplits(tableHandle, result.getPartitions()).getNextBatch(1000);
    }
}
