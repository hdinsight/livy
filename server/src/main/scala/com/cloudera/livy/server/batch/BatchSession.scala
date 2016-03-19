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

package com.cloudera.livy.server.batch

import java.lang.ProcessBuilder.Redirect
import java.util.UUID

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

import com.cloudera.livy.LivyConf
import com.cloudera.livy.recovery.SessionStore
import com.cloudera.livy.sessions.{Session, SessionState}
import com.cloudera.livy.utils.{SparkApp, SparkAppListener, SparkProcessBuilder}

object BatchSession {
  def create(
      id: Int,
      owner: String,
      request: CreateBatchRequest,
      sessionStore: SessionStore,
      livyConf: LivyConf): BatchSession = {
    val builder = transformRequestToBuilder(request, livyConf)
    val createSparkApp = { (s: BatchSession) =>
      SparkApp.create(s.uuid, builder, Option(request.file), request.args, livyConf, Option(s))
    }
    new BatchSession(id, UUID.randomUUID().toString, owner, sessionStore, createSparkApp, livyConf)
  }

  def recover(
      id: Int,
      uuid: String,
      appId: Option[String],
      owner: String,
      sessionStore: SessionStore,
      livyConf: LivyConf): BatchSession = {
    val recoverSparkApp = { (s: BatchSession) =>
      SparkApp.recover(uuid, appId, livyConf, Option(s))
    }
    new BatchSession(id, uuid, owner, sessionStore, recoverSparkApp, livyConf, appId)
  }

  private[this] def transformRequestToBuilder(request: CreateBatchRequest, livyConf: LivyConf) = {
    require(request.file != null, "File is required.")

    val builder = new SparkProcessBuilder(livyConf)
    builder.conf(request.conf)
    builder.master(livyConf.sparkMaster)
    builder.deployMode(livyConf.sparkDeployMode)
    request.proxyUser.foreach(builder.proxyUser)
    request.className.foreach(builder.className)
    request.jars.foreach(builder.jar)
    request.pyFiles.foreach(builder.pyFile)
    request.files.foreach(builder.file)
    request.driverMemory.foreach(builder.driverMemory)
    request.driverCores.foreach(builder.driverCores)
    request.executorMemory.foreach(builder.executorMemory)
    request.executorCores.foreach(builder.executorCores)
    request.numExecutors.foreach(builder.numExecutors)
    request.archives.foreach(builder.archive)
    request.queue.foreach(builder.queue)
    request.name.foreach(builder.name)
    builder.redirectOutput(Redirect.PIPE)
    builder.redirectErrorStream(true)
  }
}

class BatchSession private (
    id: Int,
    uuid: String,
    owner: String,
    sessionStore: SessionStore,
    processCreator: (BatchSession) => SparkApp,
    livyConf: LivyConf,
    appId: Option[String] = None)
  extends Session(id, owner, uuid, appId)
  with SparkAppListener {

  // Listener
  override def startingApp(): Unit = sessionStore.set(this)

  override def appIdKnown(appId: String): Unit = {
    // When we are recovering from an appId, we don't want to update the session store again.
    if (appId != _appId) {
      _appId = Option(appId)
      sessionStore.set(this)
    }
  }

  override def stateChanged(oldState: SparkApp.State.Value, newState: SparkApp.State.Value): Unit =
    {}

  private val process = processCreator(this)

  protected implicit def executor: ExecutionContextExecutor = ExecutionContext.global

  private[this] var _state: SessionState = SessionState.Running()

  override def state: SessionState = _state

  override def logLines(): IndexedSeq[String] = process.log

  override def stop(): Future[Unit] = {
    Future {
      destroyProcess()
    }
  }

  private def destroyProcess() = {
    process.stop()
    reapProcess(process.waitFor())
  }

  private def reapProcess(exitCode: Int) = synchronized {
    if (_state.isActive) {
      if (exitCode == 0) {
        _state = SessionState.Success()
      } else {
        _state = SessionState.Error()
      }
    }
  }

  // FIXME Listen to SparkAppListener instead of starting a thread.
  /** Simple daemon thread to make sure we change state when the process exits. */
  private[this] val thread = new Thread("Batch Process Reaper") {
    override def run(): Unit = {
      reapProcess(process.waitFor())
    }
  }
  thread.setDaemon(true)
  thread.start()
}
