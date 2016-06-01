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
package com.cloudera.livy.server.interactive

import scala.concurrent.Future

import org.joda.time.{DateTime, Duration}

import com.cloudera.livy.LivyConf
import com.cloudera.livy.recovery.SessionStore
import com.cloudera.livy.server.SessionServlet
import com.cloudera.livy.sessions.{Session, SessionManager}

/**
 * A session trait to provide heartbeat timestamp and a heartbeat method.
 */
trait SessionHeartbeat {
  private var _lastHeartbeat = DateTime.now()

  def heartbeat(): Unit = _lastHeartbeat = DateTime.now()

  def lastHeartbeat: DateTime = _lastHeartbeat
}

/**
 * Servlet can mixin this trait to update session's heartbeat
 * whenever a /sessions/:id REST call is made. e.g. GET /sessions/:id
 * Note: GET /sessions doesn't update heartbeats.
 */
trait SessionHeartbeatNotifier[S <: Session with SessionHeartbeat]
    extends SessionServlet[S] {

  abstract override protected def withUnprotectedSession(fn: (S => Any)): Any = {
    super.withUnprotectedSession({s =>
      s.heartbeat()
      fn(s)
    })
  }

  abstract override protected def withSession(fn: (S => Any)): Any = {
    super.withSession({s =>
      s.heartbeat()
      fn(s)
    })
  }
}

/**
 * A SessionManager trait.
 * It will create a thread and periodically delete sessions with expired heartbeat.
 * Heartbeat expiration interval is set by LivyConf.INTERACTIVE_HEARTBEAT_TIMEOUT.
 */
trait SessionHeartbeatWatchdog[S <: Session with SessionHeartbeat] {
  self: SessionManager[S] =>

  import scala.concurrent.ExecutionContext.Implicits.global

  {
    val timeout = Duration.millis(livyConf.getTimeAsMs(LivyConf.INTERACTIVE_HEARTBEAT_TIMEOUT))

    def hasHeartbeatTimeout(session: S): Boolean = session.lastHeartbeat.plus(timeout).isBeforeNow

    if (timeout != Duration.ZERO) {
      info(s"Session heartbeat timeout is set to $timeout. Starting watchdog thread.")
      val watchdogThread = new Thread(s"HeartbeatWatchdog-${self.getClass.getName}") {
        override def run(): Unit = {
          while (true) {
            sessions.values.filter(hasHeartbeatTimeout).foreach({ s =>
              info(s"Session ${s.id} expired. Last heartbeat is at ${s.lastHeartbeat}.")
              Future {
                delete(s)
              }
            })
            // CHANGE IT TO 60s before checking in.
            Thread.sleep(1 * 1000)
          }
        }
      }
      watchdogThread.setDaemon(true)
      watchdogThread.start()
    }
  }
}

/**
 * Declare a generic SessionManager class that mixin SessionHeartbeatWatchdog by default.
 */
class SessionManagerWithHeartbeat[S <: Session with SessionHeartbeat] (
    livyConf: LivyConf,
    sessionStore: Option[SessionStore] = None,
    startingId: Int = 0)
  extends SessionManager[S](livyConf, sessionStore, startingId)
  with SessionHeartbeatWatchdog[S] {}
