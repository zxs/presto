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

import io.airlift.log.Logger;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by sunwl on 2018/4/7.
 */
public class FileBasedRuleFactory
        extends AbstractRuleFactory
{
    private static final Logger log = Logger.get(FileBasedRuleFactory.class);

    @Override
    protected byte[] getJsonBytes(RuleConfig config)
            throws Throwable
    {
        return Optional.ofNullable(config.getConfigFile()).map(
                localFile -> {
                    try {
                        Path path = Paths.get(localFile);
                        if (!path.isAbsolute()) {
                            path = path.toAbsolutePath();
                        }
                        path.toFile().canRead();
                        return Files.readAllBytes(path);
                    }
                    catch (Throwable e) {
                        log.error(e);
                        throw new RuntimeException(e);
                    }
                }
        ).orElseThrow(() -> new RuntimeException("Fail to read file"));
    }

    @Override
    public Optional<RuleManager> getManager()
    {
        return Optional.of(new RuleManager()
        {
            ScheduledExecutorService service = Executors.newScheduledThreadPool(1);
            final int interval = config.getScheduledSeconds();

            @Override
            public void start()
            {
                service.scheduleAtFixedRate(() -> FileBasedRuleFactory.this.newInstance(),
                        interval, interval, TimeUnit.SECONDS);
            }

            @Override
            public void stop()
            {
                service.shutdown();
                try {
                    service.awaitTermination(10, TimeUnit.SECONDS);
                }
                catch (InterruptedException e) {
                    log.error(e);
                }
                service.shutdownNow();
            }
        });
    }
}
