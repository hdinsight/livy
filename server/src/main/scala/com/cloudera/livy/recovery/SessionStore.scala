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

import java.net.URL

import com.cloudera.livy.{LivyConf, Logging}
import com.cloudera.livy.server.batch.BatchSession
import com.cloudera.livy.server.interactive.InteractiveSession
import com.cloudera.livy.sessions.{Kind, Session}
import com.cloudera.livy.Utils.failWithLogging

case class InteractiveSessionMetadata(
  replUrl: Option[URL],
  kind: Kind,
  proxyUser: Option[String]
)

case class SessionMetadata(
  id: Int,
  uuid: String,
  owner: String,
  appId: Option[String],
  interactive: Option[InteractiveSessionMetadata] = None
)

case class SessionManagerState(nextSessionId: Int)

// Replace this with TypeTag when we migrate from Scala 2.10. TypeTag isn't thread safe in 2.10.
object SessionType extends Enumeration {
  type SessionType = Value
  val Batch = Value("batch")
  val Interactive = Value("interactive")
}

/**
 * Session store persists SessionManager's state for restart recovery and HA.
 *
 * @param livyConf
 */
class SessionStore(livyConf: LivyConf) extends Logging {
  private type SessionType = SessionType.SessionType
  private lazy val store = StateStore.get
  private var nextSessionId = 0
  private val storeVersion: String = "v1"

  /**
   * Add a session to the session state store.
   *
   * @param session The session being added.
   */
  def set(session: Session): Unit = synchronized {
    val sessionType = getSessionType(session)

    // Update nextSessionId in SessionManagerState.
    nextSessionId = Math.max(nextSessionId, session.id + 1)
    store.set(generateSessionManagerStatePath(sessionType), SessionManagerState(nextSessionId))

    // Store the session metadata.
    val interactiveMetadata = session match {
      case s: InteractiveSession => Option(InteractiveSessionMetadata(s.url, s.kind, s.proxyUser))
      case _ => None
    }
    val metadata = SessionMetadata(
      session.id,
      session.uuid,
      session.owner,
      session.appId,
      interactiveMetadata)
    store.set(generateSessionPath(session), metadata)
  }

  /**
   * Return all sessions stored in the store with specified session type.
   * If an error is thrown while retrieving a session, it logs the exception and skips that session.
   */
  def getAllSessions(sessionType: SessionType): Seq[SessionMetadata] = {
    store.getChildren(sessionType.toString).flatMap { childKey =>
      val sessionKey = s"$storeVersion/$sessionType/$childKey"
      failWithLogging(s"Failed to retrieve $sessionKey", error)
        { store.get(sessionKey, classOf[SessionMetadata]) }
    }
  }

  /**
   * Return the next unused session id with specified session type.
   * If checks the SessionManagerState stored and returns the next free session id.
   * If no SessionManagerState is stored, it returns 0.
   *
   * @throws Exception If SessionManagerState stored is corrupted, it throws an error.
   */
  def getNextSessionId(sessionType: SessionType): Int = {
    store.get(generateSessionManagerStatePath(sessionType), classOf[SessionManagerState])
      .map(_.nextSessionId).getOrElse(0)
  }

  /**
   * Remove a session from the state store.
   */
  def remove(session: Session): Unit = {
    store.remove(generateSessionPath(session))
  }

  private def generateSessionPath(session: Session): String =
    s"$storeVersion/${getSessionType(session)}/${session.id}"

  private def generateSessionManagerStatePath(sessionType: SessionType): String =
    s"$storeVersion/$sessionType"

  private def getSessionType(session: Session): SessionType = {
    session match {
      case _: BatchSession => SessionType.Batch
      case _: InteractiveSession => SessionType.Interactive
      case _ => throw new Exception(s"Unknown session type: $session")
    }
  }
}
