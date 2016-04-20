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

package com.cloudera.livy.test

import java.util.UUID

import scala.annotation.tailrec
import scala.concurrent.duration._

import com.cloudera.livy.sessions.SessionState
import com.cloudera.livy.test.framework.{BaseIntegrationTestSuite, FatalException, TestUtils}

case class StatementObject (stmt: String, expectedResult: Option[String], var stmtId: Int = -1)

class InteractiveIT extends BaseIntegrationTestSuite {

  it("Idle interactive session recovery") {
    val sessionId = livyClient.startInteractiveSession()

    val testStmts = List(
      new StatementObject("1+1", Some("res0: Int = 2")),
      new StatementObject("val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)",
        Some("org.apache.spark.sql.hive.HiveContext = org.apache.spark.sql.hive.HiveContext")))

    try {
      waitTillSessionIdle(sessionId)

      // Run the statements
      testStmts.foreach {
        runAndValidateStatement(sessionId, _)
      }
      // Now kill livy and restart
      //
      cluster.get.stopLivy()

      cluster.get.runLivy()

      val stateAfterRestart = livyClient.getInteractiveStatus(sessionId)

      stateAfterRestart should equal(SessionState.Idle().toString)

      val newTestStmts = List(
        new StatementObject("4 * 8", Some("Int = 32")),
        new StatementObject("hiveContext.sql(\\\"SELECT count(*)\\\")",
          Some("org.apache.spark.sql.DataFrame = [_c0: bigint]")))

      // Run the statements
      newTestStmts.foreach {
        runAndValidateStatement(sessionId, _)
      }

      // Verify the old statements
      testStmts.foreach({ s =>
        s.expectedResult.map({ r =>
          val result = livyClient.getStatementResult(sessionId, s.stmtId)
          if (result.indexOf(r) == -1) {
            throw new FatalException(
              s"Statement result doesn't match. Expected: $r. Actual: $result")
          }
        })
      })
    } finally {
      httpClient.prepareDelete(s"$livyEndpoint/sessions/$sessionId").execute()
    }
  }

  it("running interactive session recovery") {

    val testStmts = List(
      new StatementObject ("val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)",
        Some("org.apache.spark.sql.hive.HiveContext = org.apache.spark.sql.hive.HiveContext")))

    val sessionId = livyClient.startInteractiveSession()

    try {
      waitTillSessionIdle(sessionId)

      // Run the statements
      testStmts.foreach({ s =>
        s.stmtId = livyClient.runStatementInSession(sessionId, s.stmt)
      })
      // Now kill livy and restart
      //
      cluster.get.stopLivy()

      cluster.get.runLivy()

      val stateAfterRestart = livyClient.getInteractiveStatus(sessionId)

      stateAfterRestart should equal(SessionState.Busy().toString)

      waitTillSessionIdle(sessionId)

      // Verify the old statements
      testStmts.foreach({ s =>
        s.expectedResult.map({ r =>
          val result = livyClient.getStatementResult(sessionId, s.stmtId)
          if (result.indexOf(r) == -1) {
            throw new FatalException(
              s"Statement result doesn't match. Expected: $r. Actual: $result")
          }
        })
      })
    } finally {
      httpClient.prepareDelete(s"$livyEndpoint/sessions/$sessionId").execute()
    }
  }

  it("interactive session won't start if failed before getting appId") {
    withClue(cluster.get.getLivyLog()) {
      livyClient.createTestpoint("SessionStore.set.beforeStoringAppId", "crash")

      val sessionName = UUID.randomUUID().toString
      val sessionId = livyClient.startInteractiveSession(Some(sessionName))
      try {
        cluster.get.waitUntilLivyStop()

        waitTillJobDoneInYarn(sessionName)

        cluster.get.runLivy()

        ensureSessionDeadInLivy(sessionId)
      } finally {
        httpClient.prepareDelete(s"$livyEndpoint/sessions/$sessionId").execute()
      }
    }
  }

  it("driver fails to start") {
    withClue(cluster.get.getLivyLog()) {
      val tpId = livyClient.createTestpoint("InteractiveSession.create.badKind", "none")

      val sessionId = livyClient.startInteractiveSession()

      ensureSessionDeadInLivy(sessionId)

      livyClient.deleteTestPoint(tpId)

      httpClient.prepareDelete(s"$livyEndpoint/sessions/$sessionId").execute()
    }
  }

  it("server crash between getting Yarn ID and receiving callback") {
    withClue(cluster.get.getLivyLog()) {
      livyClient.createTestpoint("InteractiveSessionServlet.set.beforeStoringCallback", "crash")

      val sessionId = livyClient.startInteractiveSession()

      try {
        cluster.get.waitUntilLivyStop()

        cluster.get.runLivy()

        ensureSessionDeadInLivy(sessionId)
      } finally {
        httpClient.prepareDelete(s"$livyEndpoint/sessions/$sessionId").execute()
      }
    }
  }

  private def runAndValidateStatement(sessionId: Int, testStmt: StatementObject) = {
    testStmt.stmtId = livyClient.runStatementInSession(sessionId, testStmt.stmt)

    waitTillSessionIdle(sessionId)

    testStmt.expectedResult.map({ s =>
      val result = livyClient.getStatementResult(sessionId, testStmt.stmtId)
      if (result.indexOf(s) == -1) {
        throw new FatalException(s"Statement result doesn't match. Expected: $s. Actual: $result")
      }
    })
  }

  private def ensureSessionDeadInLivy(sessionId: Int): Unit =
    TestUtils.retry(5.minutes.fromNow, { () =>
      val currentState = livyClient.getInteractiveStatus(sessionId)

      currentState should equal(SessionState.Dead().toString)
    })

  private def waitTillJobDoneInYarn(sessionName: String): Unit =
    TestUtils.retry(5.minutes.fromNow, { () =>
      val activeApps = cluster.get.runCommand(s"yarn application -list")

      activeApps shouldNot contain(sessionName)
    })


  @tailrec
  private def waitTillSessionIdle (sessionId: Int): Unit = {
    val curState = livyClient.getInteractiveStatus(sessionId)
    val terminalStates = Set(SessionState.Success().toString, SessionState.Dead().toString,
      SessionState.Error().toString)

    if (terminalStates(curState)) {
      throw new FatalException(s"Session is in unexpected terminal state $curState.")
    } else if (curState != SessionState.Idle().toString) {
      Thread.sleep(1.second.toMillis)
      waitTillSessionIdle(sessionId)
    }
  }
}
