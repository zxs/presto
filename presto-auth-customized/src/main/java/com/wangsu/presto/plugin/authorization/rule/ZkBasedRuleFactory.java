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
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;

import java.io.IOException;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Created by sunwl on 2018/4/10.
 */
public class ZkBasedRuleFactory
        extends AbstractRuleFactory
{
    private static final Logger log = Logger.get(ZkBasedRuleFactory.class);

    protected static final String ZNODE = "authorization_rules.json";
    protected CuratorFramework client;
    protected PathChildrenCache cache;
    protected byte[] jsonBytes;
    protected String zkPath;

    private void initOrNot()
    {
        if (client != null) {
            return;
        }
        synchronized (this) {
            if (client != null) {
                return;
            }
            String zkConnString = requireNonNull(config.getZkConnectionString(),
                    "zk connection string is null");

            zkPath = requireNonNull(config.getZkPath(),
                    "zk path is null");

            client = CuratorFrameworkFactory.newClient(zkConnString,
                    new ExponentialBackoffRetry(1000, 3));
            if (client.getState() != CuratorFrameworkState.STARTED) {
                client.start();
            }
            cache = new PathChildrenCache(client, zkPath, true);
        }
    }

    @Override
    public Optional<RuleManager> getManager()
    {
        initOrNot();
        return Optional.of(new RuleManager()
        {
            private volatile boolean removedFromZk;

            @Override
            public void start()
            {
                try {
                    cache.start();
                    addListener(cache);
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void stop()
            {
                try {
                    cache.close();
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            private void tryUpdate(PathChildrenCacheEvent e, String path)
            {
                if (!ZNODE.equals(path)) {
                    return;
                }
                jsonBytes = e.getData().getData();
                newInstance();
            }

            private void addListener(PathChildrenCache cache)
            {
                PathChildrenCacheListener listener = (client, event) -> {
                    String path = ZKPaths.getNodeFromPath(event.getData().getPath());
                    switch (event.getType()) {
                        case CHILD_ADDED: {
                            log.info("Node added: " + path);
                            if (removedFromZk) {
                                tryUpdate(event, path);
                            }
                            break;
                        }
                        case CHILD_UPDATED: {
                            log.info("Node changed: " + path);
                            tryUpdate(event, path);
                            break;
                        }
                        case CHILD_REMOVED: {
                            log.info("Node removed: " + path);
                            removedFromZk = true;
                            break;
                        }
                    }
                };
                cache.getListenable().addListener(listener);
            }
        });
    }

    @Override
    protected byte[] getJsonBytes(RuleConfig config)
            throws Throwable
    {
        initOrNot();
        if (jsonBytes != null) {
            return jsonBytes;
        }
        return client.getData().forPath(ZKPaths.makePath(zkPath, ZNODE));
    }
}
