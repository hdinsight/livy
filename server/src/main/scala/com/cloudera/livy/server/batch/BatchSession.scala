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
import com.cloudera.livy.recovery.{RecoverableSession, SessionStore}
import com.cloudera.livy.sessions.{Session, SessionState}
import com.cloudera.livy.utils.{MetricsEmitter, SparkApp, SparkProcessBuilder}

object BatchSession {
  def create(
      id: Int,
      owner: String,
      request: CreateBatchRequest,
      sessionStore: SessionStore,
      livyConf: LivyConf): BatchSession = {
    val builder = buildRequest(request, livyConf)
    MetricsEmitter.EmitSessionStartingEvent("BatchSession", id)
    val create = { (s: BatchSession) =>
      SparkApp.create(s.uuid, builder, Option(request.file), request.args, livyConf, Option(s))
    }
    new BatchSession(id, UUID.randomUUID().toString, owner, sessionStore, create, livyConf)
  }

  def recover(
      id: Int,
      uuid: String,
      appId: Option[String],
      owner: String,
      sessionStore: SessionStore,
      livyConf: LivyConf): BatchSession = {
    MetricsEmitter.EmitSessionRecoveringEvent("BatchSession", id)
    val recover = { (s: BatchSession) =>
      SparkApp.recover(uuid, appId, livyConf, Option(s))
    }
    new BatchSession(id, uuid, owner, sessionStore, recover, livyConf, appId)
  }

  private[this] def buildRequest(request: CreateBatchRequest, livyConf: LivyConf) = {
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
    request.packages.foreach(builder.packages)

    builder.redirectOutput(Redirect.PIPE)
    builder.redirectErrorStream(true)
  }
}

class BatchSession private (
    id: Int,
    uuid: String,
    owner: String,
    val sessionStore: SessionStore,
    appCreator: (BatchSession) => SparkApp,
    livyConf: LivyConf,
    appId: Option[String] = None)
  extends Session(id, owner, uuid, appId)
  with RecoverableSession {

  private val app = appCreator(this)

  protected implicit def executor: ExecutionContextExecutor = ExecutionContext.global

  private[this] var _state: SessionState = SessionState.Running()

  override def state: SessionState = _state

  override def logLines(): IndexedSeq[String] = app.log

  override def stop(): Future[Unit] = {
    Future {
      destroyProcess()
    }
  }

  private def destroyProcess() = {
    app.stop()
    app.waitFor()
  }

  /** Fired when the app state in the cluster changes. */
  override def stateChanged(oldState: SparkApp.State, newState: SparkApp.State): Unit = {
    synchronized {
      newState match {
        case SparkApp.State.FINISHED =>
          _state = SessionState.Success()
          MetricsEmitter.EmitSessionSucceededEvent(getClass.getSimpleName, id)
        case SparkApp.State.KILLED | SparkApp.State.FAILED =>
          _state = SessionState.Dead()
          MetricsEmitter.EmitSessionFailedEvent(getClass.getSimpleName, id, newState.toString)
        case _ =>
      }
    }
  }
}
