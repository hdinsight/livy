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

package com.cloudera.livy.test.framework

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.ning.http.client.AsyncHttpClient
import javax.servlet.http.HttpServletResponse
import org.scalatest._
import scala.concurrent.duration._

import com.cloudera.livy.server.interactive.CreateInteractiveRequest
import com.cloudera.livy.server.testpoint.TestpointCreateRequest
import com.cloudera.livy.sessions.{SessionKindModule, Spark}

class FatalException(msg: String) extends Exception(msg)

class BaseIntegrationTestSuite extends Suite with BeforeAndAfterAll with FunSpecLike with Matchers {
  var cluster: Option[Cluster] = None
  val httpClient = new AsyncHttpClient()
  var livyClient: LivyClient = _

  override def beforeAll(): Unit = {
    cluster = Option(ClusterPool.get.lease())
    livyClient = new LivyClient (httpClient, livyEndpoint)
  }

  override def afterAll(): Unit = {
    cluster.foreach(ClusterPool.get.returnCluster(_))
  }

  protected def livyEndpoint: String = cluster.get.livyEndpoint

  // FIXME Should not call yarn application -status. Does not work in mini cluster mode.
  // Use YarnClient instead.
  protected def waitUntilJobFinishInYarn(appId: String): Unit =
    TestUtils.retry(5.minutes.fromNow, () => {
      val appStatus = cluster.get.runCommand(s"yarn application -status $appId")
      if (!appStatus.contains("State : FINISHED")) {
        throw new Exception(s"Yarn state is not FINISHED: It's $appStatus")
      }
    })

  class LivyClient (httpClient: AsyncHttpClient, livyEndpoint: String){

    private val mapper = new ObjectMapper()
      .registerModule(DefaultScalaModule)
      .registerModule(new SessionKindModule())

    def createTestpoint(name: String, action: String): Int = {
      val tpReq = httpClient.preparePost(s"$livyEndpoint/testpoint")
        .setBody(mapper.writeValueAsBytes(TestpointCreateRequest(name, action)))
        .execute().get()

      withClue(tpReq.getResponseBody) {
        tpReq.getStatusCode should equal(HttpServletResponse.SC_OK)

        val tpResp: Map[String, Any] =
          mapper.readValue(tpReq.getResponseBodyAsStream, classOf[Map[String, Any]])
        tpResp("id").asInstanceOf[Int]
      }
    }

    def deleteTestPoint(id: Int): Unit = {
      val tpReq = httpClient.prepareDelete(s"$livyEndpoint/testpoint/$id")
        .execute().get()

      withClue(tpReq.getResponseBody) {
        tpReq.getStatusCode should equal(HttpServletResponse.SC_OK)
      }
    }

    def getBatches(): List[Any] = {
      val r = httpClient.prepareGet(s"$livyEndpoint/batches").execute().get()
      withClue(r.getResponseBody) {
        mapper.readValue(r.getResponseBodyAsStream, classOf[List[Map[String, Any]]])
      }
    }

    def getBatchStatus(batchId: Int): String = {
      val rep = httpClient.prepareGet(s"$livyEndpoint/batches/$batchId").execute().get()
      withClue(rep.getResponseBody) {
        rep.getStatusCode should equal(HttpServletResponse.SC_OK)

        val sessionState =
          mapper.readValue(rep.getResponseBodyAsStream, classOf[Map[String, Any]])

        sessionState should contain key ("state")

        sessionState("state").asInstanceOf[String]
      }
    }

    def getBatchAppId(batchId: Int): Option[String] = {
      val rep = httpClient.prepareGet(s"$livyEndpoint/batches/$batchId").execute().get()
      withClue(rep.getResponseBody) {
        rep.getStatusCode should equal(HttpServletResponse.SC_OK)

        val sessionState =
          mapper.readValue(rep.getResponseBodyAsStream, classOf[Map[String, Any]])

        sessionState should contain key ("appId")

        Option(sessionState("appId").asInstanceOf[String])
      }
    }

    def startInteractiveSession(name: Option[String] = None): Int = {
      withClue(cluster.get.getLivyLog()) {
        val requestBody = new CreateInteractiveRequest()
        requestBody.kind = Spark()
        requestBody.name = name

        val rep = httpClient.preparePost(s"$livyEndpoint/sessions")
          .setBody(mapper.writeValueAsString(requestBody))
          .execute()
          .get()

        info("Interactive session submitted")

        val sessionId: Int = withClue(rep.getResponseBody) {
          rep.getStatusCode should equal(HttpServletResponse.SC_CREATED)
          val newSession = mapper.readValue(rep.getResponseBodyAsStream, classOf[Map[String, Any]])
          newSession should contain key ("id")

          newSession("id").asInstanceOf[Int]
        }
        info(s"Session id $sessionId")

        sessionId
      }
    }

    def getInteractiveStatus(sessionId: Int): String = {
      val rep = httpClient.prepareGet(s"$livyEndpoint/sessions/$sessionId").execute().get()
      withClue(rep.getResponseBody) {
        rep.getStatusCode should equal(HttpServletResponse.SC_OK)

        val sessionState =
          mapper.readValue(rep.getResponseBodyAsStream, classOf[Map[String, Any]])

        sessionState should contain key ("state")

        sessionState("state").asInstanceOf[String]
      }
    }

    def runStatementInSession(sessionId: Int, stmt: String): Int = {
      withClue(cluster.get.getLivyLog()) {
        val requestBody = "{ \"code\": \"" + stmt + "\" }"
        info(requestBody)
        val rep = httpClient.preparePost(s"$livyEndpoint/sessions/$sessionId/statements")
          .setBody(requestBody)
          .execute()
          .get()

        val stmtId: Int = withClue(rep.getResponseBody) {
          rep.getStatusCode should equal(HttpServletResponse.SC_CREATED)
          val newStmt = mapper.readValue(rep.getResponseBodyAsStream, classOf[Map[String, Any]])
          newStmt should contain key ("id")

          newStmt("id").asInstanceOf[Int]
        }
        stmtId
      }
    }

    def getStatementResult(sessionId: Int, stmtId: Int): String = {
      withClue(cluster.get.getLivyLog()) {
        val rep = httpClient.prepareGet(s"$livyEndpoint/sessions/$sessionId/statements/$stmtId")
          .execute()
          .get()

        val stmtResult = withClue(rep.getResponseBody) {
          rep.getStatusCode should equal(HttpServletResponse.SC_OK)
          val newStmt = mapper.readValue(rep.getResponseBodyAsStream, classOf[Map[String, Any]])
          newStmt should contain key ("output")
          val output = newStmt("output").asInstanceOf[Map[String, Any]]
          output should contain key ("data")
          val data = output("data").asInstanceOf[Map[String, Any]]
          data should contain key ("text/plain")
          data("text/plain").asInstanceOf[String]
        }

        stmtResult
      }
    }
  }
}
