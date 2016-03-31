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

import com.cloudera.livy.{LivyConf, Logging}
import com.cloudera.livy.Utils.failWithLogging
import com.cloudera.livy.server.batch.BatchSession
import com.cloudera.livy.server.interactive.InteractiveSession
import com.cloudera.livy.sessions.{Session, SessionManager}

class SessionRecovery(sessionStore: SessionStore, livyConf: LivyConf) extends Logging {
  def recover(): (SessionManager[BatchSession], SessionManager[InteractiveSession]) = {
    val batchSessionManager = recoverSessions[BatchSession](
      SessionType.Batch, { s =>
        failWithLogging(s"Failed to recover batch session $s", error) {
          Option(BatchSession.recover(s.id, s.uuid, s.appId, s.owner, sessionStore, livyConf))
        }
      })

    val interactiveSessionManager = recoverSessions[InteractiveSession](
      SessionType.Interactive, { s =>
        failWithLogging(s"Failed to recover batch session $s", error) {
          require(s.interactive.isDefined, s"Missing interactive metadata in session $s")
          val i = s.interactive.get
          Option(InteractiveSession.recover(
            s.id, s.uuid, s.appId, s.owner, i.kind, i.proxyUser, i.replUrl, sessionStore, livyConf))
        }
      }
    )

    (batchSessionManager, interactiveSessionManager)
  }

  private def recoverSessions[S<: Session](
      sessionType: SessionType.Value,
      recoveryStep: (SessionMetadata => Option[S])): SessionManager[S] = {
    val storedSessions = sessionStore.getAllSessions(sessionType)
    val nextSessionId = sessionStore.getNextSessionId(sessionType)

    val recoveredSessions = storedSessions.flatMap(recoveryStep(_))

    val sessionManager = new SessionManager[S](
      livyConf,
      Option(sessionStore),
      nextSessionId)
    recoveredSessions.foreach(sessionManager.register)
    info(s"Recovered ${recoveredSessions.length} $sessionType sessions." +
      s" Next session id: $nextSessionId")

    sessionManager
  }
}
