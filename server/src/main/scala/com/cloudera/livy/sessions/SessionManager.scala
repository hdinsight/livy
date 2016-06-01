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

package com.cloudera.livy.sessions

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

import com.cloudera.livy.{LivyConf, Logging}
import com.cloudera.livy.recovery.SessionStore
import com.cloudera.livy.utils.MetricsEmitter

object SessionManager {
  val SESSION_TIMEOUT = LivyConf.Entry("livy.server.session.timeout", "1h")
}

class SessionManager[S <: Session](
    var livyConf: LivyConf,
    sessionStore: Option[SessionStore] = None,
    startingId: Int = 0) extends Logging {

  private implicit def executor: ExecutionContext = ExecutionContext.global

  private[this] final val idCounter = new AtomicInteger(startingId)
  protected final val sessions = mutable.Map[Int, S]()

  private[this] final val sessionTimeout =
    TimeUnit.MILLISECONDS.toNanos(livyConf.getTimeAsMs(SessionManager.SESSION_TIMEOUT))

  new GarbageCollector().start()

  def nextId(): Int = idCounter.getAndIncrement()

  def register(session: S): S = {
    info(s"Registering new session ${session.getClass.getSimpleName} ${session.id}")
    synchronized {
      sessions.put(session.id, session)
    }
    session
  }

  def get(id: Int): Option[S] = sessions.get(id)

  def size(): Int = sessions.size

  def all(): Iterable[S] = sessions.values

  def delete(id: Int): Option[Future[Unit]] = {
    get(id).map(delete)
  }

  def delete(session: S): Future[Unit] = {
    info(s"Deleting new session ${session.getClass.getSimpleName} ${session.id}")
    session.stop().map { case _ =>
      synchronized {
        sessionStore.foreach(_.remove(session))
        sessions.remove(session.id)
        info(s"Deleted new session ${session.getClass.getSimpleName} ${session.id}")
        MetricsEmitter.EmitSessionDeletedEvent(session.getClass.getSimpleName, session.id)
      }
    }
  }

  def shutdown(): Unit = {
    // TODO: shut down open sessions?
  }

  def collectGarbage(): Future[Iterable[Unit]] = {
    def expired(session: Session): Boolean = {
      val currentTime = System.nanoTime()
      currentTime - session.lastActivity > math.max(sessionTimeout, session.timeout)
    }

    Future.sequence(all().filter(expired).map(delete))
  }

  private class GarbageCollector extends Thread("session gc thread") {

    setDaemon(true)

    override def run(): Unit = {
      while (true) {
        collectGarbage()
        Thread.sleep(60 * 1000)
      }
    }

  }

}
