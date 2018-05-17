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
package com.wangsu.presto.plugin.authorization.rule;

import io.airlift.configuration.Config;

/**
 * Created by sunwl on 2018/4/13.
 */
public class RuleConfig
{
    private String configFile;
    private int scheduledSeconds = 60;
    private String zkConnectionString;
    private String zkPath;
    private String alarmName;
    private String alarmExec;

    @Config("security.config-file")
    public void setConfigFile(String file)
    {
        configFile = file;
    }

    public String getConfigFile()
    {
        return configFile;
    }

    @Config("security.scheduled-seconds")
    public void setScheduledSeconds(int seconds)
    {
        scheduledSeconds = seconds;
    }

    public int getScheduledSeconds()
    {
        return scheduledSeconds;
    }

    @Config("security.zk-connection-string")
    public void setZkConnectionString(String conn)
    {
        zkConnectionString = conn;
    }

    public String getZkConnectionString()
    {
        return zkConnectionString;
    }

    @Config("security.zk-path")
    public void setZkPath(String path)
    {
        zkPath = path;
    }

    public String getZkPath()
    {
        return zkPath;
    }

    @Config("security.alarm-name")
    public void setAlarmName(String name)
    {
        alarmName = name;
    }

    public String getAlarmName()
    {
        return alarmName;
    }

    @Config("security.alarm-exec")
    public void setAlarmExec(String exec)
    {
        alarmExec = exec;
    }

    public String getAlarmExec()
    {
        return alarmExec;
    }
}
