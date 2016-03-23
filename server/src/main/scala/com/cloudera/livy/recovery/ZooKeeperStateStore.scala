/*
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.livy.recovery

import scala.collection.JavaConverters._

import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.RetryNTimes

import com.cloudera.livy.{LivyConf, Logging}
import com.cloudera.livy.LivyConf.Entry

object ZooKeeperStateStore extends StateStoreCompanion {
  private val ZK_ADDRESS_CONF = Entry("livy.server.recovery.zk.state-store.address", null)

  override def create(livyConf: LivyConf): StateStore = new ZooKeeperStateStore(livyConf)
}

class ZooKeeperStateStore(livyConf: LivyConf) extends StateStore with Logging {
  private val zkAddress = livyConf.get(ZooKeeperStateStore.ZK_ADDRESS_CONF)
  private val curatorClient = CuratorFrameworkFactory.newClient(zkAddress, new RetryNTimes(5, 100))

  Runtime.getRuntime.addShutdownHook(new Thread() {
    curatorClient.close()
  })

  curatorClient.start()

  override def set(key: String, value: Object): Unit = {
    val prefixedKey = prefixKey(key)
    val data = serializeToBytes(value)
    if (curatorClient.checkExists().forPath(prefixedKey) == null) {
      curatorClient.create().creatingParentsIfNeeded().forPath(prefixedKey, data)
    } else {
      curatorClient.setData().forPath(prefixedKey, data)
    }
  }

  override def get[T](key: String, valueType: Class[T]): Option[T] = {
    val prefixedKey = prefixKey(key)
    if (curatorClient.checkExists().forPath(prefixedKey) == null) {
      None
    } else {
      Option(deserialize(curatorClient.getData().forPath(prefixedKey), valueType))
    }
  }

  override def getChildren(key: String): Seq[String] = {
    val prefixedKey = prefixKey(key)
    if (curatorClient.checkExists().forPath(prefixedKey) == null) {
      Seq.empty[String]
    } else {
      curatorClient.getChildren.forPath(prefixedKey).asScala
    }
  }

  override def remove(key: String): Unit = {
    curatorClient.delete().guaranteed().forPath(prefixKey(key))
  }

  private def prefixKey(key: String) = {
    s"/livy/$key"
  }
}
