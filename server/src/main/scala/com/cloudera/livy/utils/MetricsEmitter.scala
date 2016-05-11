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

package com.cloudera.livy.utils

import com.cloudera.livy.Logging
import com.fasterxml.jackson.databind.ObjectMapper

object MetricsEmitter extends Logging {

  case class MetricsEvent(eventType: String,
                          sessionType: String,
                          sessionId: Int,
                          yarnState: String = "N/A")

  private val mapper = new ObjectMapper()
    .registerModule(com.fasterxml.jackson.module.scala.DefaultScalaModule)

  def EmitSessionStartingEvent (sessionType: String, sessionId: Int): Unit = {
    val eventString = mapper.writeValueAsString(
      new MetricsEvent("SessionStarting", sessionType, sessionId))

    info(s"Metrics: ${eventString}")
  }

  def EmitSessionCreatedEvent (sessionType: String, sessionId: Int): Unit = {
    val eventString = mapper.writeValueAsString(
      new MetricsEvent("SessionCreated", sessionType, sessionId))

    info(s"Metrics: ${eventString}")
  }

  def EmitSessionDeletedEvent (sessionType: String, sessionId: Int): Unit = {
    val eventString = mapper.writeValueAsString(
      new MetricsEvent("SessionDelete", sessionType, sessionId))

    info(s"Metrics: ${eventString}")
  }

  def EmitSessionRecoveringEvent (sessionType: String, sessionId: Int): Unit = {
    val eventString = mapper.writeValueAsString(
      new MetricsEvent("SessionRecovering", sessionType, sessionId))

    info(s"Metrics: ${eventString}")
  }

  def EmitSessionSucceededEvent (sessionType: String, sessionId: Int): Unit = {
    val eventString = mapper.writeValueAsString(
      new MetricsEvent("SessionSucceeded", sessionType, sessionId))

    info(s"Metrics: ${eventString}")
  }

  def EmitSessionFailedEvent (sessionType: String, sessionId: Int, yarnState: String = "N/A"): Unit = {
    val eventString = mapper.writeValueAsString(
      new MetricsEvent("SessionFailed", sessionType, sessionId, yarnState))

    info(s"Metrics: ${eventString}")
  }

}