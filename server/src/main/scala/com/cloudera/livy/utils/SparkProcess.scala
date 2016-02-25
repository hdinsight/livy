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

import java.io.{ByteArrayOutputStream, OutputStream}
import java.util.concurrent.CountDownLatch
import java.util.logging._

import scala.util.Random

import org.apache.spark.launcher.{SparkAppHandle, SparkLauncher}

import com.cloudera.livy.Logging

/**
 * Encapsulate a spark-submit process.
 * It provides state tracking & output logging of the spark-submit process.
 */
class SparkProcess(launcher: SparkLauncher) extends SparkAppHandle.Listener with Logging {
  /**
   * SparkLauncher captures the process output to a standard Java Logger.
   * This class helps capturing the process output into a string.
   * @param loggerName Name for the standard Java Logger.
   */
  private class OutputLogger(loggerName: String) {
    private class LogFormatter extends Formatter {
      def format(record: LogRecord): String = {
        s"${formatMessage(record)}\n"
      }
    }

    private val logger: Logger = Logger.getLogger(s"org.apache.spark.launcher.app.${loggerName}")
    private val logOutputStream: OutputStream = new ByteArrayOutputStream()
    private val logHandler: Handler = new StreamHandler(logOutputStream, new LogFormatter())
    logger.addHandler(logHandler)
    logger.setUseParentHandlers(false)

    def output(): IndexedSeq[String] = {
      logHandler.flush()
      // FIXME Storing the output as a string and splitting it to lines afterwards is inefficient.
      logOutputStream.toString.split("\n")
    }

    def close(): Unit = {
      logger.getHandlers.foreach(logger.removeHandler)
      logHandler.close()
      logOutputStream.close()
    }
  }

  private val (sparkAppHandle, outputLogger) = {
    // Generate a unique name for the logger to capture the process output.
    val uniqueLoggerName = s"ProcessLogger_${Random.nextInt().toString}"
    launcher.setConf("spark.launcher.childProcLoggerName", uniqueLoggerName)
    val outputLogger = new OutputLogger(uniqueLoggerName)

    // Start the Spark application.
    val sparkAppHandle = launcher.startApplication(this)
    if (sparkAppHandle == null) {
      throw new Exception("Cannot launch application")
    }
    (sparkAppHandle, outputLogger)
  }
  private val processTerminated = new CountDownLatch(1)
  private var killed = false

  def stop(): Unit = {
    if (!state.isFinal) {
      sparkAppHandle.kill()
      // After killing, sparkAppHandle.state remains unchanged.
      // We have to notify threads blocked on waitFor() manually.
      killed = true
      stateChanged(sparkAppHandle)
    }
  }

  def log(): IndexedSeq[String] = {
    outputLogger.output()
  }

  def state(): SparkAppHandle.State = {
    if (killed)
      // SparkAppHandle.kill() doesn't change its state.
      SparkAppHandle.State.KILLED
    else
      sparkAppHandle.getState
  }

  // TODO Remove this.
  def waitFor(): Unit = {
    processTerminated.await()
  }

  /** Must call this to prevent resource leakage. */
  def close(): Unit = {
    outputLogger.close()
  }

  def stateChanged(handle: SparkAppHandle): Unit = {
    info(s"New state: ${state}")
    if (state.isFinal) {
      processTerminated.countDown()
    }
  }

  def infoChanged(handle: SparkAppHandle): Unit = {
    info(s"AppId: ${sparkAppHandle.getAppId}")
  }
}
