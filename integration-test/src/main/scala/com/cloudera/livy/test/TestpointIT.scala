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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import com.cloudera.livy.server.testpoint.TestpointCreateRequest
import com.cloudera.livy.sessions.SessionKindModule
import com.cloudera.livy.test.framework.BaseIntegrationTestSuite

class TestpointIT extends BaseIntegrationTestSuite {

  private val mapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .registerModule(new SessionKindModule())

  it("should cause an exception and be delete-able") {
    val tp = TestpointCreateRequest("SessionServlet.get", "exception")
    val tpReq = httpClient.preparePost(s"$livyEndpoint/testpoint")
      .setBody(mapper.writeValueAsBytes(tp))
      .execute().get()
    val tpId = withClue(tpReq.getResponseBody) {
      tpReq.getStatusCode should equal(HttpServletResponse.SC_OK)

      val tpResp: Map[String, Any] =
        mapper.readValue(tpReq.getResponseBodyAsStream, classOf[Map[String, Any]])
      tpResp("id").asInstanceOf[Int]
    }

    {
      val rep = httpClient.prepareGet(s"$livyEndpoint/batches").execute().get()
      withClue(rep.getResponseBody) {
        rep.getStatusCode should equal(HttpServletResponse.SC_INTERNAL_SERVER_ERROR)
      }
    }

    httpClient.prepareDelete(s"$livyEndpoint/testpoint/$tpId").execute().get()

    {
      val rep = httpClient.prepareGet(s"$livyEndpoint/batches").execute().get()
      withClue(rep.getResponseBody) {
        rep.getStatusCode should equal(HttpServletResponse.SC_OK)
      }
    }
  }
}
