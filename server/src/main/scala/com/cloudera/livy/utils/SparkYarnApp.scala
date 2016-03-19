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

import java.util.concurrent.Executors
import java.util.concurrent.TimeoutException

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{blocking, Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

import org.apache.hadoop.yarn.api.records.{ApplicationId, YarnApplicationState}
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.ConverterUtils

import com.cloudera.livy.Logging
import com.cloudera.livy.util.LineBufferedProcess

object SparkYarnApp {
  private implicit val ec = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())

  private val APP_TAG_TO_ID_TIMEOUT = 5.minutes
  private val KILL_TIMEOUT = 10.second
  private val POLL_INTERVAL = 1.second

  // YarnClient is thread safe. Create once, share it across applications.
  private[this] lazy val yarnClient = YarnClient.createYarnClient()
  private[this] var yarnClientCreated = false

  def getYarnClient(): YarnClient = synchronized {
    if (!yarnClientCreated) {
      yarnClient.init(new YarnConfiguration())
      yarnClient.start()
      yarnClientCreated = true
    }
    yarnClient
  }

  def parseAppId(appId: String): ApplicationId =
    ConverterUtils.toApplicationId(appId)

  def getAppIdFromTagAsync(appTag: String): Future[ApplicationId] =
    Future { getAppIdFromTag(appTag).get }

  /**
   * Find the corresponding YARN application id from an application tag.
   *
   * @param appTag The application tag tagged on the target application.
   *               If the tag is not unique, it returns the first application it found.
   * @return ApplicationId or the failure.
   */
  @tailrec
  private def getAppIdFromTag(
    appTag: String,
    deadline: Deadline = APP_TAG_TO_ID_TIMEOUT.fromNow): Try[ApplicationId] = {
    // FIXME Should not loop thru all YARN applications but YarnClient doesn't offer an API.
    getYarnClient().getApplications().asScala.find(_.getApplicationTags.contains(appTag))
      match {
        case Some(app) => Success(app.getApplicationId)
        case None =>
          if (deadline.isOverdue) {
            Failure(new Exception(s"No YARN application is tagged with $appTag."))
          } else {
            blocking { Thread.sleep(POLL_INTERVAL.toMillis) }
            getAppIdFromTag(appTag, deadline)
          }
      }
  }
}

/**
 * Encapsulate a Spark application through YARN.
 * It provides state tracking & logging.
 *
 * @param appIdFuture A future that returns the YARN application id for this application.
 * @param process The spark-submit process launched the YARN application. This is optional.
 *                If it's provided, SparkYarnApp.log() will include its log.
 */
class SparkYarnApp(
    appIdFuture: Future[ApplicationId],
    process: Option[LineBufferedProcess],
    listener: Option[SparkAppListener])
  extends SparkApp
  with Logging {
  import SparkYarnApp.ec

  private lazy val yarnClient = SparkYarnApp.getYarnClient()
  private var state: YarnApplicationState = YarnApplicationState.NEW
  private var finalDiagnosticsLog: ArrayBuffer[String] = ArrayBuffer.empty[String]

  // TODO Instead of spawning a thread for every session, create a
  // centralized thread and batch query YARN.
  private val pollThread = new Thread(s"yarnPollThread_$this") {
    override def run() = {
      try {
        val appId = Await.result(appIdFuture, Duration.Inf)
        // Execute callback to notify upper layer the YARN application id.
        listener.foreach(_.appIdKnown(appId.toString))

        while (isRunning) {
          // Refresh application state
          val newState = yarnClient.getApplicationReport(appId).getYarnApplicationState
          if (state != newState) {
            listener.foreach(_.stateChanged(mapYarnState(state), mapYarnState(newState)))
            state = newState
          }
          Thread.sleep(SparkYarnApp.POLL_INTERVAL.toMillis)
        }

        // Log final YARN diagnostics for better error reporting.
        val finalDiagnostics = yarnClient.getApplicationReport(appId).getDiagnostics
        if (finalDiagnostics != null && !finalDiagnostics.isEmpty) {
          finalDiagnosticsLog += "---=== YARN Diagnostics ===---"
          finalDiagnosticsLog ++= finalDiagnostics.split("\n")
        }

        info(s"$appId $state $finalDiagnostics")
      } catch {
        case e: InterruptedException =>
          finalDiagnosticsLog = ArrayBuffer("Session stopped by user.")
          state = YarnApplicationState.KILLED
        case e: Throwable =>
          error(s"Error whiling polling YARN state: $e")
          finalDiagnosticsLog = ArrayBuffer(e.toString)
          state = YarnApplicationState.FAILED
      }
    }
  }

  pollThread.start()

  override def stop(): Unit = {
    if (isRunning) {
      try {
        Await.result(appIdFuture.map(yarnClient.killApplication), SparkYarnApp.KILL_TIMEOUT)
      } catch {
        // If we don't have the application id, try out best to kill the application.
        // There's a chance the YARN application was lost.
        case _: TimeoutException | _: InterruptedException =>
          warn("Deleting a session while its YARN application is not found.")
          process.foreach(_.destroy())
          pollThread.interrupt()
      }
    }
  }

  override def log(): IndexedSeq[String] =
    process.map(_.inputLines).getOrElse(ArrayBuffer.empty[String]) ++ finalDiagnosticsLog

  override def waitFor(): Int = {
    pollThread.join()

    state match {
      case YarnApplicationState.FINISHED => 0
      case YarnApplicationState.FAILED | YarnApplicationState.KILLED => 1
      case _ =>
        error(s"Unexpected YARN state ${appIdFuture.value.get.get} $state")
        1
    }
  }

  override def appId: Option[String] = appIdFuture.value.flatMap(_.toOption).map(_.toString)

  private def isRunning: Boolean = {
    state != YarnApplicationState.FAILED &&
    state != YarnApplicationState.FINISHED &&
    state != YarnApplicationState.KILLED
  }

  private def mapYarnState(yarnApplicationState: YarnApplicationState): SparkApp.State.Value = {
    yarnApplicationState match {
      case (YarnApplicationState.NEW |
            YarnApplicationState.NEW_SAVING |
            YarnApplicationState.SUBMITTED |
            YarnApplicationState.ACCEPTED) => SparkApp.State.STARTING
      case YarnApplicationState.RUNNING => SparkApp.State.RUNNING
      case YarnApplicationState.FINISHED => SparkApp.State.FINISHED
      case YarnApplicationState.FAILED => SparkApp.State.FAILED
      case YarnApplicationState.KILLED => SparkApp.State.KILLED
    }
  }
}
