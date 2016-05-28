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

import java.util.List;

public class JdbcSqlParameters
{
    private final String sql;
    private final List<Object> params;

    public JdbcSqlParameters(String sql, List<Object> params)
    {
        this.sql = sql;
        this.params = params;
    }

    public String getSql()
    {
        return sql;
    }

    public List<Object> getParams()
    {
        return params;
    }
}
