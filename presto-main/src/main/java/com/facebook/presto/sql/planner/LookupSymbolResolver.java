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
package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.nio.charset.StandardCharsets.UTF_8;

public class LookupSymbolResolver
        implements SymbolResolver
{
    private final Map<Symbol, Object> bindings;

    public LookupSymbolResolver(Map<Symbol, Object> bindings)
    {
        checkNotNull(bindings, "bindings is null");

        this.bindings = ImmutableMap.copyOf(bindings);
    }

    @Override
    public Object getValue(Symbol symbol)
    {
        if (!bindings.containsKey(symbol)) {
            return new QualifiedNameReference(symbol.toQualifiedName());
        }

        Object value = bindings.get(symbol);

        if (value instanceof String) {
            return Slices.wrappedBuffer(((String) value).getBytes(UTF_8));
        }

        return value;
    }
}
