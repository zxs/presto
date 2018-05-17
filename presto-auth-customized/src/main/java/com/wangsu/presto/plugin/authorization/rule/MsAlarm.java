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

import com.google.common.io.CharStreams;
import com.google.inject.Inject;
import io.airlift.log.Logger;

import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.MoreObjects.firstNonNull;
import static java.util.Objects.requireNonNull;

/**
 * Created by sunwl on 2018/4/18.
 */
public class MsAlarm
{
    private static final Logger log = Logger.get(MsAlarm.class);
    private static final String EXEC_DEFAULT = "/usr/bin/msalarm";

    private String exec;
    private String name;
    private boolean enabled;

    @Inject
    public MsAlarm(RuleConfig config)
    {
        try {
            exec = new File(firstNonNull(config.getAlarmExec(), EXEC_DEFAULT)).getAbsolutePath();

            if (!Files.exists(Paths.get(exec))) {
                throw new RuntimeException(String.format("%s does not exist, the alarm is disabled", exec));
            }
            name = requireNonNull(config.getAlarmName());
            enabled = true;
        }
        catch (Throwable e) {
            log.error(e);
        }
    }

    public void alert(String message)
    {
        try {
            log.info("[Alert] %s", message);
            if (!enabled) {
                log.warn("Alarm is disabled, not sending alarm.");
                return;
            }
            emmit(message);
        }
        catch (Throwable e) {
            log.error(e);
        }
    }

    private void emmit(String message)
            throws Throwable
    {
        Process process = Runtime.getRuntime()
                .exec(new String[] {
                        exec,
                        "-n",
                        name,
                        "-p",
                        message
                });
        process.waitFor(5, TimeUnit.SECONDS);
        try (InputStream is = process.getInputStream();
                Reader reader = new InputStreamReader(is)) {
            log.info(CharStreams.toString(reader));
        }
        process.destroy();
    }
}
