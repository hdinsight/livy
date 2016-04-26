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

import com.cloudera.livy.sessions.Session
import com.cloudera.livy.utils.{AppInfo, SparkAppListener}

trait RecoverableSession extends SparkAppListener { self: Session =>
  val sessionStore: SessionStore
  override def startingApp(): Unit = sessionStore.set(this)

  override def appIdKnown(appId: String): Unit = {
    // When we are recovering from an appId, we don't want to update the session store again.
    if (Option(appId) != _appId) {
      _appId = Option(appId)
      sessionStore.set(this)
    }
  }

  // TODO Move this to another trait.
  override def infoChanged(newAppInfo: AppInfo): Unit =
    appInfo = newAppInfo
}
