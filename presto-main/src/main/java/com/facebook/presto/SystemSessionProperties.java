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
package com.facebook.presto;

public final class SystemSessionProperties
{
    private static final String BIG_QUERY = "experimental_big_query";
    private static final String OPTIMIZE_HASH_GENERATION = "optimize_hash_generation";
    private static final String DISTRIBUTED_JOIN = "distributed_join";

    private SystemSessionProperties() {}

    public static boolean isBigQueryEnabled(Session session, boolean defaultValue)
    {
        return isEnabled(BIG_QUERY, session, defaultValue);
    }

    private static boolean isEnabled(String propertyName, Session session, boolean defaultValue)
    {
        String enabled = session.getSystemProperties().get(propertyName);
        if (enabled == null) {
            return defaultValue;
        }

        return Boolean.valueOf(enabled);
    }

    public static boolean isOptimizeHashGenerationEnabled(Session session, boolean defaultValue)
    {
        return isEnabled(OPTIMIZE_HASH_GENERATION, session, defaultValue);
    }

    public static boolean isDistributedJoinEnabled(Session session, boolean defaultValue)
    {
        return isEnabled(DISTRIBUTED_JOIN, session, defaultValue);
    }
}
