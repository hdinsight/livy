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

import java.io.File
import java.nio.file.{Files, Paths, StandardCopyOption}

import scala.collection.mutable.Map

import com.cloudera.livy.{LivyConf, Logging}
import com.cloudera.livy.LivyConf.Entry

object FileStateStore extends StateStoreCompanion {
  private val FILE_PATH_CONF = Entry("livy.server.recovery.file.state-store.path", "livy-state")

  override def create(livyConf: LivyConf): StateStore = new FileStateStore(livyConf)
}

class FileStateStore(livyConf: LivyConf) extends StateStore with Logging {
  private val filePath = livyConf.get(FileStateStore.FILE_PATH_CONF)

  private val store: Map[String, String] = {
    val file = new File(filePath)
    if (file.exists()) {
      mapper.readValue(file, classOf[Map[String, String]])
    } else {
      Map.empty[String, String]
    }
  }

  override def set(key: String, value: Object): Unit = synchronized {
    store(key) = serialize(value)
    persist()
  }

  override def get[T](key: String, valueType: Class[T]): Option[T] = synchronized {
    store.get(key).map(deserialize[T](_, valueType))
  }

  override def getChildren(key: String): Seq[String] = synchronized {
    val keyPattern = s"$key/+(.+?)(?:/.*)*".r
    store.keys.flatMap(keyPattern.findFirstMatchIn(_)).map(_.group(1)).toSeq
  }

  override def remove(key: String): Unit = synchronized {
    store.remove(key)
    persist()
  }

  private def persist(): Unit = {
    val tempFile = File.createTempFile("livy", "json")
    mapper.writeValue(tempFile, store)
    Files.move(tempFile.toPath, Paths.get(filePath), StandardCopyOption.ATOMIC_MOVE)
  }
}
