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

package com.cloudera.livy.utils

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import com.cloudera.livy.{LivyConf, Logging}
import com.cloudera.livy.server.testpoint.TestpointManager
import com.cloudera.livy.util.LineBufferedProcess

case class AppInfo(var driverLogUrl: Option[String] = None, var sparkUiUrl: Option[String] = None)

trait SparkAppListener {
  /**
   * Fired only when a new app on the cluster is being launched.
   * It won't be fired uring recovery.
   */
  def startingApp(): Unit = {}

  /** Fired when appId is known, even during recovery. */
  def appIdKnown(appId: String): Unit = {}

  /** Fired when the app state in the cluster changes. */
  def stateChanged(oldState: SparkApp.State, newState: SparkApp.State): Unit = {}

  /** Fired when the app info is changed. */
  def infoChanged(appInfo: AppInfo): Unit = {}
}

/**
 * Provide factory methods for SparkApp.
 */
object SparkApp extends Logging {
  import SparkYarnApp.getAppIdFromTagAsync

  // This class looks quite similar to SparkLauncher. Consider adding recovery to SparkLauncher.
  object State extends Enumeration {
    val STARTING, RUNNING, FINISHED, FAILED, KILLED = Value
  }
  type State = State.Value

  /**
   * Call this to create a new Spark application.
   * It also automatically configure YARN configurations if necessary.
   */
  def create(
      uuid: String,
      builder: SparkProcessBuilder,
      file: Option[String],
      args: List[String],
      livyConf: LivyConf,
      listener: Option[SparkAppListener]): SparkApp = {
    if (livyConf.isSparkMasterYarn) {
      val appTag = uuidToAppTag(uuid)
      builder.conf("spark.yarn.tags", appTag)
      builder.conf("spark.yarn.maxAppAttempts", "1")

      listener.foreach(_.startingApp())
      val process: Option[LineBufferedProcess] = {
        if (TestpointManager.get.checkpoint("SparkApp.create.skipSparkSubmit")) {
          None
        } else {
          Option(builder.start(file, args))
        }
      }
      new SparkYarnApp(getAppIdFromTagAsync(uuidToAppTag(uuid)), process, listener)
    } else {
      new SparkProcApp(builder.start(file, args), listener)
    }
  }

  /**
   * Call this to recover an existing Spark application.
   * It needs an handle to the existing application, which could be a session uuid or an app id.
   * It works only on YARN.
   */
  def recover(
      uuid: String,
      appId: Option[String],
      livyConf: LivyConf,
      listener: Option[SparkAppListener]): SparkApp = {
    assert(livyConf.isSparkMasterYarn, "Recovery is only supported on YARN.")

    val appIdRetriever = appId match {
      case Some(appId: String) => Future { SparkYarnApp.parseAppId(appId) }
      case None => getAppIdFromTagAsync(uuidToAppTag(uuid))
    }
    new SparkYarnApp(appIdRetriever, None, listener)
  }

  // TODO change - to _
  private def uuidToAppTag(uuid: String): String = s"livy_$uuid"
}

/**
 * Encapsulate a Spark application.
 */
abstract class SparkApp() {
  def isRunning: Boolean
  def stop(): Unit
  def log(): IndexedSeq[String]
  def waitFor(): Int
  def appId: Option[String]
}
