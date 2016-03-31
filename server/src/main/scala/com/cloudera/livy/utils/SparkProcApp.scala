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

import com.cloudera.livy.util.LineBufferedProcess

/**
 * Encapsulate a Spark application through the spark-submit process launching the spark application.
 * It provides state tracking & logging.
 *
 * @param process The spark-submit process launched the Spark application.
 */
class SparkProcApp(
    process: LineBufferedProcess,
    listener: Option[SparkAppListener])
  extends SparkApp {

  private var state = SparkApp.State.STARTING

  override def isRunning: Boolean = process.isAlive

  override def stop(): Unit = {
    if (process.isAlive) {
      process.destroy()
    }
  }

  override def log(): IndexedSeq[String] = process.inputLines

  override def waitFor(): Int = {
    waitThread.join()
    process.exitValue()
  }

  // TODO Migrate to SparkLauncher and return SparkAppHandle.getAppId()
  override def appId: Option[String] = None

  private def changeState(newState: SparkApp.State.Value) = {
    if (state != newState) {
      listener.foreach(_.stateChanged(state, newState))
      state = newState
    }
  }

  val waitThread = new Thread(new Runnable {
    override def run(): Unit = {
      changeState(SparkApp.State.RUNNING)
      process.waitFor() match {
        case 0 => changeState(SparkApp.State.FINISHED)
        case _ => changeState(SparkApp.State.FAILED)
      }
    }
  }, s"SparProcApp_$this")

  waitThread.setDaemon(true)
  waitThread.start()
}
