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
package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.ObjectArrays;
import io.airlift.slice.Slice;

import javax.annotation.Nullable;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Iterables.transform;
import static java.util.Objects.requireNonNull;

public class QueryBuilder
{
    private final String quote;

    public QueryBuilder(String quote)
    {
        this.quote = requireNonNull(quote, "quote is null");
    }

    public JdbcSqlParameters buildSql(String catalog, String schema, String table, List<JdbcColumnHandle> columns, TupleDomain<ColumnHandle> tupleDomain)
    {
        StringBuilder sql = new StringBuilder();

        sql.append("SELECT ");
        Joiner.on(", ").appendTo(sql, transform(columns, column -> quote(column.getColumnName())));
        if (columns.isEmpty()) {
            sql.append("null");
        }

        sql.append(" FROM ");
        if (!isNullOrEmpty(catalog)) {
            sql.append(quote(catalog)).append('.');
        }
        if (!isNullOrEmpty(schema)) {
            sql.append(quote(schema)).append('.');
        }
        sql.append(quote(table));
        List<Object> parameters = Lists.newArrayList();
        List<String> clauses = toConjuncts(columns, tupleDomain, parameters);
        if (!clauses.isEmpty()) {
            sql.append(" WHERE ")
                    .append(Joiner.on(" AND ").join(clauses));
        }

        return new JdbcSqlParameters(sql.toString(), parameters);
    }

    private List<String> toConjuncts(List<JdbcColumnHandle> columns, TupleDomain<ColumnHandle> tupleDomain, List<Object> parameters)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (JdbcColumnHandle column : columns) {
            Type type = column.getColumnType();
            if (type.equals(BigintType.BIGINT) || type.equals(DoubleType.DOUBLE) || type.equals(BooleanType.BOOLEAN) || type.equals(VarcharType.VARCHAR)) {
                Domain domain = tupleDomain.getDomains().get().get(column);
                if (domain != null) {
                    builder.add(toPredicate(column.getColumnName(), type, domain, parameters));
                }
            }
        }
        return builder.build();
    }

    private String toPredicate(String columnName, Type columnType, Domain domain, List<Object> parameters)
    {
        checkArgument(domain.getType().isOrderable(), "Domain type must be orderable");

        if (domain.getValues().isNone()) {
            return domain.isNullAllowed() ? quote(columnName) + " IS NULL" : "FALSE";
        }

        if (domain.getValues().isAll()) {
            return domain.isNullAllowed() ? "TRUE" : quote(columnName) + " IS NOT NULL";
        }

        List<String> disjuncts = new ArrayList<>();
        List<Object> singleValues = new ArrayList<>();
        for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
            checkState(!range.isAll()); // Already checked
            if (range.isSingleValue()) {
                singleValues.add(range.getLow().getValue());
            }
            else {
                List<String> rangeConjuncts = new ArrayList<>();
                if (!range.getLow().isLowerUnbounded()) {
                    switch (range.getLow().getBound()) {
                        case ABOVE:
                            rangeConjuncts.add(toPredicate(columnName, "> ?"/*range.getLow().getValue()*/));
                            parameters.add(encode(range.getLow().getValue(), columnType));
                            break;
                        case EXACTLY:
                            rangeConjuncts.add(toPredicate(columnName, ">= ?"/*range.getLow().getValue()*/));
                            parameters.add(encode(range.getLow().getValue(), columnType));
                            break;
                        case BELOW:
                            throw new IllegalArgumentException("Low Marker should never use BELOW bound: " + range);
                        default:
                            throw new AssertionError("Unhandled bound: " + range.getLow().getBound());
                    }
                }
                if (!range.getHigh().isUpperUnbounded()) {
                    switch (range.getHigh().getBound()) {
                        case ABOVE:
                            throw new IllegalArgumentException("High Marker should never use ABOVE bound: " + range);
                        case EXACTLY:
                            rangeConjuncts.add(toPredicate(columnName, "<= ?"/*, range.getHigh().getValue()*/));
                            parameters.add(encode(range.getHigh().getValue(), columnType));
                            break;
                        case BELOW:
                            rangeConjuncts.add(toPredicate(columnName, "< ?"/*, range.getHigh().getValue()*/));
                            parameters.add(encode(range.getHigh().getValue(), columnType));
                            break;
                        default:
                            throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
                    }
                }
                // If rangeConjuncts is null, then the range was ALL, which should already have been checked for
                checkState(!rangeConjuncts.isEmpty());
                disjuncts.add("(" + Joiner.on(" AND ").join(rangeConjuncts) + ")");
            }
        }

        // Add back all of the possible single values either as an equality or an IN predicate
        if (singleValues.size() == 1) {
            disjuncts.add(toPredicate(columnName, "= ?"/*, getOnlyElement(singleValues)*/));
            parameters.add(encode(getOnlyElement(singleValues), columnType));
        }
        else if (singleValues.size() > 1) {
            String[] tbd = ObjectArrays.newArray(String.class, singleValues.size());
            Arrays.fill(tbd, "?");
            disjuncts.add(quote(columnName) + " IN (" + Joiner.on(",").join(tbd/*transform(singleValues, encode())*/) + ")");
            MyFunction myFunc = new MyFunction(columnType);
            Iterators.addAll(parameters, transform(singleValues, myFunc).iterator());
        }

        // Add nullability disjuncts
        checkState(!disjuncts.isEmpty());
        if (domain.isNullAllowed()) {
            disjuncts.add(quote(columnName) + " IS NULL");
        }

        return "(" + Joiner.on(" OR ").join(disjuncts) + ")";
    }

    private String toPredicate(String columnName, String operator/*, Object value*/)
    {
        return quote(columnName) + " " + operator + " "/* + encode(value)*/;
    }

    private String quote(String name)
    {
        name = name.replace(quote, quote + quote);
        return quote + name + quote;
    }

    private Object encode(Object value, Type type)
    {
        String valueStr = null;
        if (value instanceof Number || value instanceof Boolean) {
            valueStr = value.toString();
        }
        if (value instanceof Slice && ((Slice) value).getBase() instanceof byte[]) {
          valueStr = new String((byte[]) (((Slice) value).getBase()));
        }
        // jdbc sql type convert
        if (type.equals(BooleanType.BOOLEAN)) {
          return Boolean.valueOf(valueStr);
        }
        else if (type.equals(BigintType.BIGINT)) {
          return Long.valueOf(valueStr);
        }
        else if (type.equals(DoubleType.DOUBLE)) {
          return Double.valueOf(valueStr);
        }
        else if (type.equals(VarcharType.VARCHAR)) {
          return valueStr;
        }
        else if (type.equals(VarbinaryType.VARBINARY)) {
          // nothing to do
        }
        else if (type.equals(DateType.DATE)) {
          return Date.valueOf(valueStr);
        }
        else if (type.equals(TimeType.TIME)) {
          return Time.valueOf(valueStr);
        }
        else if (type.equals(TimestampType.TIMESTAMP)) {
          return Timestamp.valueOf(valueStr);
        }
        throw new UnsupportedOperationException("Can't handle type: " + value.getClass().getName() + type);
    }

    private class MyFunction implements Function<Object, Object>
    {
        private Type type;
        public MyFunction(Type type)
        {
            this.type = type;
        }

        @Nullable
        @Override
        public Object apply(@Nullable Object value)
        {
            return encode(value, type);
        }
    }
}
