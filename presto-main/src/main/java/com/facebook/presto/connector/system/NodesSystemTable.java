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
package com.facebook.presto.connector.system;

import com.facebook.presto.metadata.AllNodes;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.PrestoNode;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.InMemoryRecordSet.Builder;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;

public class NodesSystemTable
        implements SystemTable
{
    public static final SchemaTableName NODES_TABLE_NAME = new SchemaTableName("sys", "node");

    public static final ConnectorTableMetadata NODES_TABLE = tableMetadataBuilder(NODES_TABLE_NAME)
            .column("node_id", VARCHAR)
            .column("http_uri", VARCHAR)
            .column("node_version", VARCHAR)
            .column("is_active", BOOLEAN)
            .build();

    private final InternalNodeManager nodeManager;

    @Inject
    public NodesSystemTable(InternalNodeManager nodeManager)
    {
        this.nodeManager = checkNotNull(nodeManager, "nodeManager is null");
    }

    @Override
    public boolean isDistributed()
    {
        return false;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return NODES_TABLE;
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return ImmutableList.copyOf(transform(NODES_TABLE.getColumns(), ColumnMetadata::getType));
    }

    @Override
    public RecordCursor cursor()
    {
        Builder table = InMemoryRecordSet.builder(NODES_TABLE);
        AllNodes allNodes = nodeManager.getAllNodes();
        for (Node node : allNodes.getActiveNodes()) {
            table.addRow(node.getNodeIdentifier(), node.getHttpUri().toString(), getNodeVersion(node), Boolean.TRUE);
        }
        for (Node node : allNodes.getInactiveNodes()) {
            table.addRow(node.getNodeIdentifier(), node.getHttpUri().toString(), getNodeVersion(node), Boolean.FALSE);
        }
        return table.build().cursor();
    }

    private static String getNodeVersion(Node node)
    {
        if (node instanceof PrestoNode) {
            return ((PrestoNode) node).getNodeVersion().toString();
        }
        return "";
    }
}
