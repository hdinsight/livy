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

package com.cloudera.livy.server.testpoint

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._

import com.cloudera.livy.Logging

object TestpointManager {
  private val testpointManager: TestpointManager = synchronized {
    new TestpointManager
  }

  def get: TestpointManager = {
    testpointManager
  }
}

class TestpointManager extends Logging {
  private case class Testpoint(locationName: String, condition: Map[String, Any], action: String)

  private val testpoints = new ConcurrentHashMap[Int, Testpoint]().asScala
  private val testpointId = new AtomicInteger(0)

  def create(locationName: String, condition: Map[String, Any], action: String): Int = {
    val newId = testpointId.getAndIncrement()
    testpoints.put(newId, Testpoint(locationName, condition, action))

    newId
  }

  def remove(testpointId: Int): Unit = testpoints.remove(testpointId)

  def checkpoint(locationName: String, variables: Map[String, Any] = Map.empty): Boolean = {
    val testpoint = testpoints.values.find({ tp =>
      tp.locationName == locationName
      // TODO tp.condition should be a subset of variables
    })

    testpoint.foreach(tp => execute(tp.action))
    testpoint.isDefined
  }

  private def execute(action: String): Unit = action match {
    case "none" =>
    case "crash" =>
      info("Hitting a crash Testpoint. Killing livy-server.")
      System.exit(0)
    case "exception" => throw new Exception("Testpoint injected exception")
    case _ => warn(s"Unknown testpoint action $action")
  }
}
