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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanRewriter;

import java.util.Map;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;

/**
 * Turns RIGHT joins into LEFT joins
 */
public class NormalizeJoinOrder
        extends PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        return PlanRewriter.rewriteWith(new PlanRewriter<Void>()
        {
            @Override
            public PlanNode visitJoin(JoinNode node, RewriteContext<Void> context)
            {
                if (node.getType() != JoinNode.Type.RIGHT) {
                    return context.defaultRewrite(node);
                }

                // return a new join node with "left" swapped with "right"
                return new JoinNode(node.getId(),
                        JoinNode.Type.LEFT,
                        context.rewrite(node.getRight()),
                        context.rewrite(node.getLeft()),
                        node.getCriteria().stream()
                                .map(criteria -> new JoinNode.EquiJoinClause(criteria.getRight(), criteria.getLeft()))
                                .collect(toImmutableList()),
                        node.getRightHashSymbol(),
                        node.getLeftHashSymbol());
            }
        }, plan);
    }
}
