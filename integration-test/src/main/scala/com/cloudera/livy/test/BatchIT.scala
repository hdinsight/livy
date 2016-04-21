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

import javax.servlet.http.HttpServletResponse

import scala.concurrent.duration._

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.ning.http.client.{ListenableFuture, Response}

import com.cloudera.livy.server.batch.CreateBatchRequest
import com.cloudera.livy.sessions.{SessionKindModule, SessionState}
import com.cloudera.livy.test.framework.{BaseIntegrationTestSuite, TestUtils}

class BatchIT extends BaseIntegrationTestSuite {

  private val mapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .registerModule(new SessionKindModule())

  it("can run PySpark script") {
    withClue(cluster.get.getLivyLog()) {
      val batchId = submitBatchJob("pyspark-test.py", List.empty)
      info(s"Batch id $batchId")
      ensureBatchSuccessInLivy(batchId)

      info(s"Batch job finished")
    }
  }

  it("recovers running batch session") {
    withClue(cluster.get.getLivyLog()) {
      val batchId = submitBatchJob("sleep.py", List("1200"))
      val appId: String = waitForAppId(batchId)

      info(s"Batch AppId $appId")

      cluster.get.stopLivy()
      cluster.get.runLivy()

      val stateAfterRestart = livyClient.getBatchStatus(batchId)
      stateAfterRestart should equal(SessionState.Running().toString)

      httpClient.prepareDelete(s"$livyEndpoint/batches/$batchId").execute()
    }
  }

  it("recovers finished batch session") {
    withClue(cluster.get.getLivyLog()) {
      val batchId = submitBatchJob("sleep.py", List("5"))
      val appId: String = waitForAppId(batchId)

      info(s"Batch AppId $appId")

      cluster.get.stopLivy()

      // Make sure job is finished before restarting Livy
      waitUntilJobFinishInYarn(appId)
      cluster.get.runLivy()

      ensureBatchSuccessInLivy(batchId)
    }
  }

  it("cannot recover batch session if server crashed before calling spark-submit") {
    withClue(cluster.get.getLivyLog()) {
      livyClient.createTestpoint("SparkApp.create.skipSparkSubmit", "none")

      val batchId = submitBatchJob("pyspark-test.py", List.empty)
      cluster.get.stopLivy()
      cluster.get.runLivy()

      TestUtils.retry(2.minute.fromNow, () => {
        livyClient.getBatchStatus(batchId) should equal(SessionState.Dead().toString)
      })
    }
  }

  it("recovers batch session when server crashed before getting yarn app id") {
    withClue(cluster.get.getLivyLog()) {
      livyClient.createTestpoint("SessionStore.set.beforeStoringAppId", "crash")

      val batchId = submitBatchJob("pyspark-test.py", List.empty)
      cluster.get.waitUntilLivyStop()
      cluster.get.runLivy()

      val stateAfterRestart = livyClient.getBatchStatus(batchId)
      stateAfterRestart should equal(SessionState.Running().toString)
      httpClient.prepareDelete(s"$livyEndpoint/batches/$batchId").execute()
    }
  }

  it("recovers batch session when server crashed after getting yarn app id") {
    withClue(cluster.get.getLivyLog()) {
      livyClient.createTestpoint("SessionStore.set.afterStoringAppId", "crash")

      val batchId = submitBatchJob("pyspark-test.py", List.empty)
      cluster.get.waitUntilLivyStop()
      cluster.get.runLivy()

      ensureBatchSuccessInLivy(batchId)

      httpClient.prepareDelete(s"$livyEndpoint/batches/$batchId").execute()
    }
  }

  private def submitBatchJobRequest(pyScriptName: String, args: List[String]):
    ListenableFuture[Response] = {
    val requestBody = new CreateBatchRequest()
    requestBody.file = uploadPySparkTestScript(pyScriptName)
    requestBody.args = args

    httpClient.preparePost(s"$livyEndpoint/batches")
      .setBody(mapper.writeValueAsString(requestBody))
      .execute()
  }

  private def submitBatchJob(pyScriptName: String, args: List[String]): Int = {
    val rep = submitBatchJobRequest(pyScriptName, args).get()

    val batchId: Int = withClue(rep.getResponseBody) {
      rep.getStatusCode should equal(HttpServletResponse.SC_CREATED)
      val newSession = mapper.readValue(rep.getResponseBodyAsStream, classOf[Map[String, Any]])
      newSession should contain key ("id")

      newSession("id").asInstanceOf[Int]
    }
    info(s"Batch id $batchId")
    batchId
  }

  private def uploadPySparkTestScript(fileName: String): String = {
    val tmpFile = TestUtils.saveTestSourceToTempFile(fileName)

    val destPath = s"/tmp/upload-${tmpFile.getName}"
    cluster.get.upload(tmpFile.getAbsolutePath, destPath)

    tmpFile.delete()
    destPath
  }

  private def waitForAppId(batchId: Int): String =
    TestUtils.retry(5.minutes.fromNow, () => livyClient.getBatchAppId(batchId).get)


  private def ensureBatchSuccessInLivy(batchId: Int): Unit =
    TestUtils.retry(5.minutes.fromNow, { () =>
      val currentState = livyClient.getBatchStatus(batchId)

      val validStates = Set(
        SessionState.Starting(),
        SessionState.Running(),
        SessionState.Success()).map(_.toString)

      if (!validStates(currentState)) {
        throw new Error(s"Job is in unexpected state. $currentState $validStates")
      }

      currentState should equal(SessionState.Success().toString)
    })
}
