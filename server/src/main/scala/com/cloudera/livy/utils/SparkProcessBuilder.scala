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

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, Map}

import org.apache.spark.launcher.SparkLauncher

import com.cloudera.livy.{LivyConf, Logging}
import com.cloudera.livy.util.LineBufferedProcess

class SparkProcessBuilder(livyConf: LivyConf) extends Logging {
  private[this] var _executable: String = livyConf.sparkSubmit()
  private[this] var _master: Option[String] = None
  private[this] var _deployMode: Option[String] = None
  private[this] var _className: Option[String] = None
  private[this] var _name: Option[String] = Some("Livy")
  private[this] var _jars: ArrayBuffer[String] = ArrayBuffer()
  private[this] var _pyFiles: ArrayBuffer[String] = ArrayBuffer()
  private[this] var _files: ArrayBuffer[String] = ArrayBuffer()
  private[this] val _conf = mutable.HashMap[String, String]()
  private[this] var _driverClassPath: ArrayBuffer[String] = ArrayBuffer()
  private[this] var _proxyUser: Option[String] = None

  private[this] var _queue: Option[String] = None
  private[this] var _archives: ArrayBuffer[String] = ArrayBuffer()

  private[this] var _env: Map[String, String] = Map()
  private[this] var _redirectOutput: Option[ProcessBuilder.Redirect] = None
  private[this] var _redirectError: Option[ProcessBuilder.Redirect] = None
  private[this] var _redirectErrorStream: Option[Boolean] = None

  def executable(executable: String): SparkProcessBuilder = {
    _executable = executable
    this
  }

  def master(masterUrl: String): SparkProcessBuilder = {
    _master = Some(masterUrl)
    this
  }

  def deployMode(deployMode: String): SparkProcessBuilder = {
    _deployMode = Some(deployMode)
    this
  }

  def className(className: String): SparkProcessBuilder = {
    _className = Some(className)
    this
  }

  def name(name: String): SparkProcessBuilder = {
    _name = Some(name)
    this
  }

  def jar(jar: String): SparkProcessBuilder = {
    this._jars += jar
    this
  }

  def jars(jars: Traversable[String]): SparkProcessBuilder = {
    this._jars ++= jars
    this
  }

  def pyFile(pyFile: String): SparkProcessBuilder = {
    this._pyFiles += pyFile
    this
  }

  def pyFiles(pyFiles: Traversable[String]): SparkProcessBuilder = {
    this._pyFiles ++= pyFiles
    this
  }

  def file(file: String): SparkProcessBuilder = {
    this._files += file
    this
  }

  def files(files: Traversable[String]): SparkProcessBuilder = {
    this._files ++= files
    this
  }

  def conf(key: String): Option[String] = {
    _conf.get(key)
  }

  def conf(key: String, value: String, admin: Boolean = false): SparkProcessBuilder = {
    this._conf(key) = value
    this
  }

  def conf(conf: Traversable[(String, String)]): SparkProcessBuilder = {
    conf.foreach { case (key, value) => this.conf(key, value) }
    this
  }

  def driverJavaOptions(driverJavaOptions: String): SparkProcessBuilder = {
    conf("spark.driver.extraJavaOptions", driverJavaOptions)
  }

  def driverClassPath(classPath: String): SparkProcessBuilder = {
    _driverClassPath += classPath
    this
  }

  def driverClassPaths(classPaths: Traversable[String]): SparkProcessBuilder = {
    _driverClassPath ++= classPaths
    this
  }

  def driverCores(driverCores: Int): SparkProcessBuilder = {
    this.driverCores(driverCores.toString)
  }

  def driverMemory(driverMemory: String): SparkProcessBuilder = {
    conf("spark.driver.memory", driverMemory)
  }

  def driverCores(driverCores: String): SparkProcessBuilder = {
    conf("spark.driver.cores", driverCores)
  }

  def executorCores(executorCores: Int): SparkProcessBuilder = {
    this.executorCores(executorCores.toString)
  }

  def executorCores(executorCores: String): SparkProcessBuilder = {
    conf("spark.executor.cores", executorCores)
  }

  def executorMemory(executorMemory: String): SparkProcessBuilder = {
    conf("spark.executor.memory", executorMemory)
  }

  def numExecutors(numExecutors: Int): SparkProcessBuilder = {
    this.numExecutors(numExecutors.toString)
  }

  def numExecutors(numExecutors: String): SparkProcessBuilder = {
    this.conf("spark.executor.instances", numExecutors)
  }

  def proxyUser(proxyUser: String): SparkProcessBuilder = {
    _proxyUser = Some(proxyUser)
    this
  }

  def queue(queue: String): SparkProcessBuilder = {
    _queue = Some(queue)
    this
  }

  def archive(archive: String): SparkProcessBuilder = {
    _archives += archive
    this
  }

  def archives(archives: Traversable[String]): SparkProcessBuilder = {
    archives.foreach(archive)
    this
  }

  def env(key: String, value: String): SparkProcessBuilder = {
    _env += (key -> value)
    this
  }

  def redirectOutput(redirect: ProcessBuilder.Redirect): SparkProcessBuilder = {
    _redirectOutput = Some(redirect)
    this
  }

  def redirectError(redirect: ProcessBuilder.Redirect): SparkProcessBuilder = {
    _redirectError = Some(redirect)
    this
  }

  def redirectErrorStream(redirect: Boolean): SparkProcessBuilder = {
    _redirectErrorStream = Some(redirect)
    this
  }

  def start(file: Option[String], args: Traversable[String]): LineBufferedProcess = {
    val launcher: SparkLauncher = new SparkLauncher(_env.asJava)

    _master.foreach(launcher.setMaster)
    _deployMode.foreach(launcher.setDeployMode)
    _name.foreach(launcher.setAppName)
    _jars.foreach(launcher.addJar)
    _pyFiles.foreach(launcher.addPyFile)
    _files.foreach(launcher.addFile)
    _className.foreach(launcher.setMainClass)
    _conf.foreach { case (key, value) =>
      launcher.setConf(key, value)
    }
    _driverClassPath.foreach(launcher.setConf("spark.driver.extraClassPath", _))

    if (livyConf.getBoolean(LivyConf.IMPERSONATION_ENABLED)) {
      _proxyUser.foreach(launcher.addSparkArg("--proxy-user", _))
    }

    _queue.foreach(launcher.addSparkArg("--queue", _))
    _archives.foreach(launcher.addSparkArg("--archives", _))

    launcher.setAppResource(file.getOrElse("spark-internal"))
    launcher.addAppArgs(args.toArray: _*)

    // Some customers ask for this for diagnosing issues.
    info(s"Calling SparkLauncher ${generateSparkSubmitCmd(file, args)}")

    new LineBufferedProcess(launcher.launch())
  }

  private def generateSparkSubmitCmd(file: Option[String], args: Traversable[String]): String = {
    var cmdLine = ArrayBuffer(_executable)

    def addOpt(option: String, value: Option[String]): Unit = {
      value.foreach { v =>
        cmdLine += option
        cmdLine += v
      }
    }

    def addList(option: String, values: Traversable[String]): Unit = {
      if (values.nonEmpty) {
        cmdLine += option
        cmdLine += values.mkString(",")
      }
    }

    addOpt("--master", _master)
    addOpt("--deploy-mode", _deployMode)
    addOpt("--name", _name)
    addList("--jars", _jars)
    addList("--py-files", _pyFiles)
    addList("--files", _files)
    addOpt("--class", _className)
    _conf.foreach { case (key, value) =>
      cmdLine += "--conf"
      cmdLine += f"$key=$value"
    }
    addList("--driver-class-path", _driverClassPath)

    if (livyConf.getBoolean(LivyConf.IMPERSONATION_ENABLED)) {
      addOpt("--proxy-user", _proxyUser)
    }

    addOpt("--queue", _queue)
    addList("--archives", _archives)

    cmdLine += file.getOrElse("spark-internal")
    cmdLine ++= args

    cmdLine
      .map("'" + _.replace("'", "\\'") + "'")
      .mkString(" ")
  }
}
