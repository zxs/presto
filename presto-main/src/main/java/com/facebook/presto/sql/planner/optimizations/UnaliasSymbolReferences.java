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
import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.DeterminismEvaluator;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.IndexSourceNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanRewriter;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Re-maps symbol references that are just aliases of each other (e.g., due to projections like {@code $0 := $1})
 * <p/>
 * E.g.,
 * <p/>
 * {@code Output[$0, $1] -> Project[$0 := $2, $1 := $3 * 100] -> Aggregate[$2, $3 := sum($4)] -> ...}
 * <p/>
 * gets rewritten as
 * <p/>
 * {@code Output[$2, $1] -> Project[$2, $1 := $3 * 100] -> Aggregate[$2, $3 := sum($4)] -> ...}
 */
public class UnaliasSymbolReferences
        extends PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        checkNotNull(plan, "plan is null");
        checkNotNull(session, "session is null");
        checkNotNull(types, "types is null");
        checkNotNull(symbolAllocator, "symbolAllocator is null");
        checkNotNull(idAllocator, "idAllocator is null");

        return PlanRewriter.rewriteWith(new Rewriter(new HashMap<Symbol, Symbol>()), plan);
    }

    private static class Rewriter
            extends PlanRewriter<Void>
    {
        private final Map<Symbol, Symbol> mapping;

        public Rewriter(Map<Symbol, Symbol> mapping)
        {
            this.mapping = mapping;
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());

            ImmutableMap.Builder<Symbol, Signature> functionInfos = ImmutableMap.builder();
            ImmutableMap.Builder<Symbol, FunctionCall> functionCalls = ImmutableMap.builder();
            ImmutableMap.Builder<Symbol, Symbol> masks = ImmutableMap.builder();
            for (Map.Entry<Symbol, FunctionCall> entry : node.getAggregations().entrySet()) {
                Symbol symbol = entry.getKey();
                Symbol canonical = canonicalize(symbol);
                FunctionCall canonicalCall = (FunctionCall) canonicalize(entry.getValue());
                functionCalls.put(canonical, canonicalCall);
                functionInfos.put(canonical, node.getFunctions().get(symbol));
            }
            for (Map.Entry<Symbol, Symbol> entry : node.getMasks().entrySet()) {
                masks.put(canonicalize(entry.getKey()), canonicalize(entry.getValue()));
            }

            List<Symbol> groupByKeys = ImmutableList.copyOf(ImmutableSet.copyOf(canonicalize(node.getGroupBy())));
            return new AggregationNode(node.getId(), source, groupByKeys, functionCalls.build(), functionInfos.build(), masks.build(), canonicalize(node.getSampleWeight()), node.getConfidence(), node.getHashSymbol());
        }

        @Override
        public PlanNode visitMarkDistinct(MarkDistinctNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());
            List<Symbol> symbols = ImmutableList.copyOf(ImmutableSet.copyOf(canonicalize(node.getDistinctSymbols())));
            return new MarkDistinctNode(node.getId(), source, canonicalize(node.getMarkerSymbol()), symbols, node.getHashSymbol());
        }

        @Override
        public PlanNode visitUnnest(UnnestNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());
            ImmutableMap.Builder<Symbol, List<Symbol>> builder = ImmutableMap.builder();
            for (Map.Entry<Symbol, List<Symbol>> entry : node.getUnnestSymbols().entrySet()) {
                builder.put(canonicalize(entry.getKey()), canonicalize(entry.getValue()));
            }
            return new UnnestNode(node.getId(), source, canonicalize(node.getReplicateSymbols()), builder.build());
        }

        @Override
        public PlanNode visitWindow(WindowNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());

            ImmutableMap.Builder<Symbol, Signature> functionInfos = ImmutableMap.builder();
            ImmutableMap.Builder<Symbol, FunctionCall> functionCalls = ImmutableMap.builder();
            for (Map.Entry<Symbol, FunctionCall> entry : node.getWindowFunctions().entrySet()) {
                Symbol symbol = entry.getKey();
                Symbol canonical = canonicalize(symbol);
                functionCalls.put(canonical, (FunctionCall) canonicalize(entry.getValue()));
                functionInfos.put(canonical, node.getSignatures().get(symbol));
            }

            ImmutableMap.Builder<Symbol, SortOrder> orderings = ImmutableMap.builder();
            for (Map.Entry<Symbol, SortOrder> entry : node.getOrderings().entrySet()) {
                orderings.put(canonicalize(entry.getKey()), entry.getValue());
            }

            WindowNode.Frame frame = node.getFrame();
            frame = new WindowNode.Frame(frame.getType(),
                    frame.getStartType(), canonicalize(frame.getStartValue()),
                    frame.getEndType(), canonicalize(frame.getEndValue()));

            return new WindowNode(node.getId(), source, canonicalize(node.getPartitionBy()), canonicalize(node.getOrderBy()), orderings.build(), frame, functionCalls.build(), functionInfos.build(), node.getHashSymbol());
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Void> context)
        {
            ImmutableMap.Builder<Symbol, ColumnHandle> builder = ImmutableMap.builder();
            for (Map.Entry<Symbol, ColumnHandle> entry : node.getAssignments().entrySet()) {
                builder.put(canonicalize(entry.getKey()), entry.getValue());
            }

            Expression originalConstraint = null;
            if (node.getOriginalConstraint() != null) {
                originalConstraint = canonicalize(node.getOriginalConstraint());
            }
            return new TableScanNode(node.getId(), node.getTable(), canonicalize(node.getOutputSymbols()), builder.build(), originalConstraint, node.getSummarizedPartition());
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());

            return new FilterNode(node.getId(), source, canonicalize(node.getPredicate()));
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());

            Map<Expression, Symbol> computedExpressions = new HashMap<>();

            Map<Symbol, Expression> assignments = new LinkedHashMap<>();
            for (Map.Entry<Symbol, Expression> entry : node.getAssignments().entrySet()) {
                Expression expression = canonicalize(entry.getValue());

                if (entry.getValue() instanceof QualifiedNameReference) {
                    // Always map a trivial symbol projection
                    Symbol symbol = Symbol.fromQualifiedName(((QualifiedNameReference) entry.getValue()).getName());
                    if (!symbol.equals(entry.getKey())) {
                        map(entry.getKey(), symbol);
                    }
                }
                else if (DeterminismEvaluator.isDeterministic(expression) && !(expression instanceof NullLiteral)) {
                    // Try to map same deterministic expressions within a projection into the same symbol
                    // Omit NullLiterals since those have ambiguous types
                    Symbol computedSymbol = computedExpressions.get(expression);
                    if (computedSymbol == null) {
                        // If we haven't seen the expression before in this projection, record it
                        computedExpressions.put(expression, entry.getKey());
                    }
                    else {
                        // If we have seen the expression before and if it is deterministic
                        // then we can rewrite references to the current symbol in terms of the parallel computedSymbol in the projection
                        map(entry.getKey(), computedSymbol);
                    }
                }

                Symbol canonical = canonicalize(entry.getKey());

                if (!assignments.containsKey(canonical)) {
                    assignments.put(canonical, expression);
                }
            }

            return new ProjectNode(node.getId(), source, assignments);
        }

        @Override
        public PlanNode visitOutput(OutputNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());

            List<Symbol> canonical = Lists.transform(node.getOutputSymbols(), this::canonicalize);
            return new OutputNode(node.getId(), source, node.getColumnNames(), canonical);
        }

        @Override
        public PlanNode visitTopN(TopNNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());

            ImmutableList.Builder<Symbol> symbols = ImmutableList.builder();
            ImmutableMap.Builder<Symbol, SortOrder> orderings = ImmutableMap.builder();
            for (Symbol symbol : node.getOrderBy()) {
                Symbol canonical = canonicalize(symbol);
                symbols.add(canonical);
                orderings.put(canonical, node.getOrderings().get(symbol));
            }

            return new TopNNode(node.getId(), source, node.getCount(), symbols.build(), orderings.build(), node.isPartial());
        }

        @Override
        public PlanNode visitSort(SortNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());

            ImmutableList.Builder<Symbol> symbols = ImmutableList.builder();
            ImmutableMap.Builder<Symbol, SortOrder> orderings = ImmutableMap.builder();
            for (Symbol symbol : node.getOrderBy()) {
                Symbol canonical = canonicalize(symbol);
                symbols.add(canonical);
                orderings.put(canonical, node.getOrderings().get(symbol));
            }

            return new SortNode(node.getId(), source, symbols.build(), orderings.build());
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Void> context)
        {
            PlanNode left = context.rewrite(node.getLeft());
            PlanNode right = context.rewrite(node.getRight());

            return new JoinNode(node.getId(), node.getType(), left, right, canonicalizeJoinCriteria(node.getCriteria()), node.getLeftHashSymbol(), node.getRightHashSymbol());
        }

        @Override
        public PlanNode visitSemiJoin(SemiJoinNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());
            PlanNode filteringSource = context.rewrite(node.getFilteringSource());

            return new SemiJoinNode(node.getId(), source, filteringSource, canonicalize(node.getSourceJoinSymbol()), canonicalize(node.getFilteringSourceJoinSymbol()), canonicalize(node.getSemiJoinOutput()), node.getSourceHashSymbol(), node.getFilteringSourceHashSymbol());
        }

        @Override
        public PlanNode visitIndexSource(IndexSourceNode node, RewriteContext<Void> context)
        {
            ImmutableMap.Builder<Symbol, ColumnHandle> builder = ImmutableMap.builder();
            for (Map.Entry<Symbol, ColumnHandle> entry : node.getAssignments().entrySet()) {
                builder.put(canonicalize(entry.getKey()), entry.getValue());
            }
            return new IndexSourceNode(node.getId(), node.getIndexHandle(), node.getTableHandle(), canonicalize(node.getLookupSymbols()), canonicalize(node.getOutputSymbols()), builder.build(), node.getEffectiveTupleDomain());
        }

        @Override
        public PlanNode visitIndexJoin(IndexJoinNode node, RewriteContext<Void> context)
        {
            PlanNode probeSource = context.rewrite(node.getProbeSource());
            PlanNode indexSource = context.rewrite(node.getIndexSource());

            return new IndexJoinNode(node.getId(), node.getType(), probeSource, indexSource, canonicalizeIndexJoinCriteria(node.getCriteria()), node.getProbeHashSymbol(), node.getIndexHashSymbol());
        }

        @Override
        public PlanNode visitUnion(UnionNode node, RewriteContext<Void> context)
        {
            ImmutableList.Builder<PlanNode> rewrittenSources = ImmutableList.builder();
            for (PlanNode source : node.getSources()) {
                rewrittenSources.add(context.rewrite(source));
            }

            return new UnionNode(node.getId(), rewrittenSources.build(), canonicalizeUnionSymbolMap(node.getSymbolMapping()));
        }

        @Override
        public PlanNode visitTableWriter(TableWriterNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());

            return new TableWriterNode(node.getId(), source, node.getTarget(), canonicalize(node.getColumns()), node.getColumnNames(), canonicalize(node.getOutputSymbols()), canonicalize(node.getSampleWeightSymbol()));
        }

        private void map(Symbol symbol, Symbol canonical)
        {
            Preconditions.checkArgument(!symbol.equals(canonical), "Can't map symbol to itself: %s", symbol);
            mapping.put(symbol, canonical);
        }

        private Optional<Symbol> canonicalize(Optional<Symbol> symbol)
        {
            if (symbol.isPresent()) {
                return Optional.of(canonicalize(symbol.get()));
            }
            return Optional.empty();
        }

        private Symbol canonicalize(Symbol symbol)
        {
            Symbol canonical = symbol;
            while (mapping.containsKey(canonical)) {
                canonical = mapping.get(canonical);
            }
            return canonical;
        }

        private Expression canonicalize(Expression value)
        {
            return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
            {
                @Override
                public Expression rewriteQualifiedNameReference(QualifiedNameReference node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
                {
                    Symbol canonical = canonicalize(Symbol.fromQualifiedName(node.getName()));
                    return new QualifiedNameReference(canonical.toQualifiedName());
                }
            }, value);
        }

        private List<Symbol> canonicalize(List<Symbol> outputs)
        {
            return Lists.transform(outputs, this::canonicalize);
        }

        private Set<Symbol> canonicalize(Set<Symbol> symbols)
        {
            return FluentIterable.from(symbols)
                    .transform(this::canonicalize)
                    .toSet();
        }

        private List<JoinNode.EquiJoinClause> canonicalizeJoinCriteria(List<JoinNode.EquiJoinClause> criteria)
        {
            ImmutableList.Builder<JoinNode.EquiJoinClause> builder = ImmutableList.builder();
            for (JoinNode.EquiJoinClause clause : criteria) {
                builder.add(new JoinNode.EquiJoinClause(canonicalize(clause.getLeft()), canonicalize(clause.getRight())));
            }

            return builder.build();
        }

        private List<IndexJoinNode.EquiJoinClause> canonicalizeIndexJoinCriteria(List<IndexJoinNode.EquiJoinClause> criteria)
        {
            ImmutableList.Builder<IndexJoinNode.EquiJoinClause> builder = ImmutableList.builder();
            for (IndexJoinNode.EquiJoinClause clause : criteria) {
                builder.add(new IndexJoinNode.EquiJoinClause(canonicalize(clause.getProbe()), canonicalize(clause.getIndex())));
            }

            return builder.build();
        }

        private ListMultimap<Symbol, Symbol> canonicalizeUnionSymbolMap(ListMultimap<Symbol, Symbol> unionSymbolMap)
        {
            ImmutableListMultimap.Builder<Symbol, Symbol> builder = ImmutableListMultimap.builder();
            for (Map.Entry<Symbol, Collection<Symbol>> entry : unionSymbolMap.asMap().entrySet()) {
                builder.putAll(canonicalize(entry.getKey()), Iterables.transform(entry.getValue(), this::canonicalize));
            }
            return builder.build();
        }
    }
}
