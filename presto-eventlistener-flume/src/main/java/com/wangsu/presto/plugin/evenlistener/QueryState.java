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
package com.wangsu.presto.plugin.evenlistener;

public enum QueryState
{
    /**
     * Query has been accepted and is awaiting execution.
     */
    QUEUED,
    /**
     * Query is being planned.
     */
    PLANNING,
    /**
     * Query execution is being started.
     */
    STARTING,
    /**
     * Query has at least one running task.
     */
    RUNNING,
    /**
     * Query is finishing (e.g. commit for autocommit queries)
     */
    FINISHING,
    /**
     * Query has finished executing and all output has been consumed.
     */
    FINISHED,
    /**
     * Query execution failed.
     */
    FAILED
}
