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
package com.wangsu.presto.plugin.authorization;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.io.FileInputStream;
import java.io.InputStream;

/**
 * Created by sunwl on 2018/4/16.
 */
public class TestHelper
{
    private TestHelper() {}
    public static void setZkData(String config, String zkConn, String zkPath)
            throws Exception
    {
        byte[] data;
        try (InputStream is = new FileInputStream(config);) {
            data = new byte[is.available()];
            is.read(data);
            is.close();
        }
        finally {
            //pass
        }
        CuratorFramework client = CuratorFrameworkFactory.newClient(zkConn,
                new ExponentialBackoffRetry(1000, 3));
        if (client.getState() != CuratorFrameworkState.STARTED) {
            client.start();
        }
        if (client.checkExists().forPath(zkPath) == null) {
            client.create().forPath(zkPath, data);
        }
        else {
            client.setData().forPath(zkPath, data);
        }
    }
}
