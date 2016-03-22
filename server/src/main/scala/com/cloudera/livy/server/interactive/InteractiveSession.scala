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

package com.cloudera.livy.server.interactive

import java.io.File
import java.lang.ProcessBuilder.Redirect
import java.net.{ConnectException, URL}
import java.nio.file.{Files, Paths}
import java.util.UUID
import java.util.concurrent.TimeUnit

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Future, _}
import scala.concurrent.duration.Duration

import dispatch._
import org.json4s.{DefaultFormats, Formats, JValue}
import org.json4s.JsonAST.{JNull, JString}
import org.json4s.jackson.Serialization.write

import com.cloudera.livy.{ExecuteRequest, LivyConf, Logging, Utils}
import com.cloudera.livy.recovery.{RecoverableSession, SessionStore}
import com.cloudera.livy.sessions._
import com.cloudera.livy.sessions.interactive.Statement
import com.cloudera.livy.utils.{SparkApp, SparkProcessBuilder}

object InteractiveSession {
  val LivyReplAdditionalFiles = "livy.repl.additional.files"
  val LivyReplDriverClassPath = "livy.repl.driverClassPath"
  val LivyReplJars = "livy.repl.jars"
  val LivyServerUrl = "livy.server.serverUrl"
  val SparkDriverExtraJavaOptions = "spark.driver.extraJavaOptions"
  val SparkLivyCallbackUrl = "spark.livy.callbackUrl"
  val SparkLivyPort = "spark.livy.port"
  val SparkSubmitPyFiles = "spark.submit.pyFiles"
  val SparkYarnIsPython = "spark.yarn.isPython"

  def create(
      id: Int,
      owner: String,
      livyConf: LivyConf,
      request: CreateInteractiveRequest,
      sessionStore: SessionStore): InteractiveSession = {
    val builder: SparkProcessBuilder = buildRequest(id, livyConf, request)
    val kind = request.kind

    val create = { (s: InteractiveSession) =>
      SparkApp.create(s.uuid, builder, None, List(kind.toString), livyConf, Option(s))
    }

    val uuid = UUID.randomUUID().toString
    val proxyUser = request.proxyUser
    new InteractiveSession(id, uuid, owner, kind, proxyUser, create, sessionStore, livyConf, false)
  }

  def recover(
      id: Int,
      uuid: String,
      appId: Option[String],
      owner: String,
      kind: Kind,
      proxyUser: Option[String],
      replUrl: Option[URL],
      sessionStore: SessionStore,
      livyConf: LivyConf): InteractiveSession = {
    val recover = { (s: InteractiveSession) =>
      SparkApp.recover(uuid, appId, livyConf, Option(s))
    }
    new InteractiveSession(
      id, uuid, owner, kind, proxyUser, recover, sessionStore, livyConf, true, appId, replUrl)
  }

  private[this] def buildRequest(
    id: Int,
    livyConf: LivyConf,
    request: CreateInteractiveRequest): SparkProcessBuilder = {
    val builder = new SparkProcessBuilder(livyConf)
    builder.className("com.cloudera.livy.repl.Main")
    builder.conf(request.conf)
    builder.master(livyConf.sparkMaster)
    builder.deployMode(livyConf.sparkDeployMode)
    request.archives.foreach(builder.archive)
    request.driverCores.foreach(builder.driverCores)
    request.driverMemory.foreach(builder.driverMemory)
    request.executorCores.foreach(builder.executorCores)
    request.executorMemory.foreach(builder.executorMemory)
    request.numExecutors.foreach(builder.numExecutors)
    request.files.foreach(builder.file)

    val jars = request.jars ++ livyJars(livyConf)
    jars.foreach(builder.jar)

    request.proxyUser.foreach(builder.proxyUser)
    request.queue.foreach(builder.queue)
    request.name.foreach(builder.name)

    request.kind match {
      case PySpark() =>
        builder.conf(SparkYarnIsPython, "true", admin = true)

        // FIXME: Spark-1.4 seems to require us to manually upload the PySpark support files.
        // We should only do this for Spark 1.4.x
        val pySparkFiles = if (!LivyConf.TEST_MODE) findPySparkArchives() else Nil
        builder.files(pySparkFiles)

        // We can't actually use `builder.pyFiles`, because livy-repl is a Jar, and
        // spark-submit will reject it because it isn't a Python file. Instead we'll pass it
        // through a special property that the livy-repl will use to expose these libraries in
        // the Python shell.
        builder.files(request.pyFiles)

        builder.conf(SparkSubmitPyFiles, (pySparkFiles ++ request.pyFiles).mkString(","),
          admin = true)
      case _ =>
    }

    sys.env.get("LIVY_REPL_JAVA_OPTS").foreach { replJavaOpts =>
      val javaOpts = builder.conf(SparkDriverExtraJavaOptions) match {
        case Some(javaOptions) => f"$javaOptions $replJavaOpts"
        case None => replJavaOpts
      }
      builder.conf(SparkDriverExtraJavaOptions, javaOpts, admin = true)
    }

    Option(livyConf.get(LivyReplAdditionalFiles))
      .foreach(builder.file)

    Option(livyConf.get(LivyReplDriverClassPath))
      .foreach(builder.driverClassPath)

    sys.props.get(LivyServerUrl).foreach { serverUrl =>
      val callbackUrl = f"$serverUrl/sessions/$id/callback"
      builder.conf(SparkLivyCallbackUrl, callbackUrl, admin = true)
    }

    builder.conf(SparkLivyPort, "0", admin = true)

    builder.redirectOutput(Redirect.PIPE)
    builder.redirectErrorStream(true)
  }

  private[this] def livyJars(livyConf: LivyConf): Seq[String] = {
    Option(livyConf.get(LivyReplJars)).map(_.split(",").toSeq).getOrElse {
      val home = sys.env("LIVY_HOME")
      val jars = Option(new File(home, "repl-jars"))
        .filter(_.isDirectory())
        .getOrElse(new File(home, "repl/target/jars"))
      require(jars.isDirectory(), "Cannot find Livy REPL jars.")
      jars.listFiles().map(_.getAbsolutePath()).toSeq
    }
  }

  private[this] def findPySparkArchives(): Seq[String] = {
    sys.env.get("PYSPARK_ARCHIVES_PATH")
      .map(_.split(",").toSeq)
      .getOrElse {
        sys.env.get("SPARK_HOME") .map { case sparkHome =>
          val pyLibPath = Seq(sparkHome, "python", "lib").mkString(File.separator)
          val pyArchivesFile = new File(pyLibPath, "pyspark.zip")
          require(pyArchivesFile.exists(),
            "pyspark.zip not found; cannot run pyspark application in YARN mode.")

          val py4jFile = Files.newDirectoryStream(Paths.get(pyLibPath), "py4j-*-src.zip")
            .iterator()
            .next()
            .toFile

          require(py4jFile.exists(),
            "py4j-*-src.zip not found; cannot run pyspark application in YARN mode.")
          Seq(pyArchivesFile.getAbsolutePath, py4jFile.getAbsolutePath)
        }.getOrElse(Seq())
      }
  }
}

class InteractiveSession private (
    id: Int,
    uuid: String,
    owner: String,
    val kind: Kind,
    val proxyUser: Option[String],
    appCreator: (InteractiveSession) => SparkApp,
    val sessionStore: SessionStore,
    livyConf: LivyConf,
    recovery: Boolean,
    appId: Option[String] = None,
    private[this] var _url: Option[URL] = None)
  extends Session(id, owner, uuid, appId)
  with RecoverableSession
  with Logging {

  private val app = appCreator(this)
  private val pendingStatementCountMutex = AnyRef
  private var pendingStatementCount = 0
  private var serverSideLog = ArrayBuffer.empty[String]

  protected implicit def executor: ExecutionContextExecutor =
    ExecutionContext.fromExecutor(new scala.concurrent.forkjoin.ForkJoinPool(2))
  protected implicit def jsonFormats: Formats = DefaultFormats

  protected[this] var _state: SessionState = SessionState.Starting()

  private[this] var _executedStatements = 0
  private[this] var _statements = IndexedSeq[Statement]()

  override def logLines(): IndexedSeq[String] = app.log ++ serverSideLog

  override def state: SessionState = _state

  override def stop(): Future[Unit] = {
    val future: Future[Unit] = synchronized {
      _state match {
        case SessionState.Idle() =>
          _state = SessionState.Busy()

          Http(svc.DELETE OK as.String).either() match {
            case (Right(_) | Left(_: ConnectException)) =>
              // Make sure to eat any connection errors because the repl shut down before it sent
              // out an OK.
              synchronized {
                _state = SessionState.Dead()
              }

              Future.successful(())

            case Left(t: Throwable) =>
              Future.failed(t)
          }
        case SessionState.NotStarted() =>
          Future {
            waitForStateChange(SessionState.NotStarted(), Duration(10, TimeUnit.SECONDS))
            stop()
          }
        case SessionState.Starting() =>
          Future {
            waitForStateChange(SessionState.Starting(), Duration(10, TimeUnit.SECONDS))
            stop()
          }
        case SessionState.Busy() | SessionState.Running() =>
          Future {
            waitForStateChange(SessionState.Busy(), Duration(10, TimeUnit.SECONDS))
            stop()
          }
        case SessionState.ShuttingDown() =>
          Future {
            waitForStateChange(SessionState.ShuttingDown(), Duration(10, TimeUnit.SECONDS))
            stop()
          }
        case SessionState.Error(_) | SessionState.Dead(_) | SessionState.Success(_) =>
          Future {
            app.stop()
            Future.successful(Unit)
          }
      }
    }

    future.andThen { case r =>
      app.waitFor()
      r
    }
  }

  def url: Option[URL] = _url

  def url_=(url: URL): Unit = {
    ensureState(SessionState.Starting(), {
      _state = SessionState.Idle()
      _url = Option(url)
      sessionStore.set(this)
    })
  }

  def statements: IndexedSeq[Statement] = _statements

  def interrupt(): Future[Unit] = {
    stop()
  }

  def executeStatement(content: ExecuteRequest): Statement = {
    ensureRunning {
      newPendingStatement()
      recordActivity()

      val req = (svc / "execute").setContentType("application/json", "UTF-8") << write(content)

      info(s"Executing statement ${id}")
      val future = Http(req OK as.json4s.Json).map { case resp: JValue =>
        parseResponse(resp).getOrElse {
          // The result isn't ready yet. Loop until it is.
          val id = (resp \ "id").extract[Int]
          waitForStatement(id)
        }
      }

      val statement = new Statement(_executedStatements, content, future)

      _executedStatements += 1
      _statements = _statements :+ statement

      statement
    }
  }

  def waitForStateChange(oldState: SessionState, atMost: Duration): Unit = {
    Utils.waitUntil({ () => state != oldState }, atMost)
  }

  private def svc = {
    val url = _url.head
    dispatch.url(url.toString)
  }

  @tailrec
  private def waitForStatement(id: Int): JValue = {
    val req = (svc / "history" / id).setContentType("application/json", "UTF-8")
    val resp = Await.result(Http(req OK as.json4s.Json), Duration.Inf)

    parseResponse(resp) match {
      case Some(result) => result
      case None =>
        Thread.sleep(1000)
        waitForStatement(id)
    }
  }

  private def parseResponse(response: JValue): Option[JValue] = {
    response \ "result" match {
      case JNull => None
      case result =>
        // If the response errored out, it's possible it took down the interpreter. Check if
        // it's still running.
        result \ "status" match {
          case JString("error") =>
            if (replErroredOut()) {
              pendingStatementCountMutex.synchronized {
                pendingStatementCount = 0
              }
              transition(SessionState.Error())
            } else {
              endPendingStatement()
            }
          case _ => endPendingStatement()
        }

        Some(result)
    }
  }

  private def replErroredOut() = {
    val req = svc.setContentType("application/json", "UTF-8")
    val response = Await.result(Http(req OK as.json4s.Json), Duration.Inf)

    response \ "state" match {
      case JString("error") => true
      case _ => false
    }
  }

  private def transition(state: SessionState) = synchronized {
    _state = state
  }

  private def ensureState[A](state: SessionState, f: => A) = {
    synchronized {
      if (_state == state) {
        f
      } else {
        throw new IllegalStateException("Session is in state %s" format _state)
      }
    }
  }

  private def ensureRunning[A](f: => A) = {
    synchronized {
      _state match {
        case SessionState.Idle() | SessionState.Busy() =>
          f
        case _ =>
          throw new IllegalStateException("Session is in state %s" format _state)
      }
    }
  }

  private def newPendingStatement(): Unit = {
    pendingStatementCountMutex.synchronized {
      transition(SessionState.Busy())
      pendingStatementCount += 1
    }
  }

  private def endPendingStatement(): Unit = {
    pendingStatementCountMutex.synchronized {
      pendingStatementCount -= 1
      assert(pendingStatementCount >= 0)
      if (pendingStatementCount == 0) {
        transition(SessionState.Idle())
      }
    }
  }

  private def startStatementsRecovery(): Future[Unit] = {
    Future {
      val req = (svc / "history").setContentType("application/json", "UTF-8") <<?
        Map("size" -> Int.MaxValue.toString)
      val resp = Await.result(Http(req OK as.json4s.Json), Duration.Inf)

      _executedStatements = (resp \ "total").extract[Int]
      val statements = (resp \ "statements").extract[List[JValue]]

      transition(SessionState.Idle())

      _statements = statements.map(s => {
        val id = (s \ "id").extract[Int]
        val result = (s \ "result").extract[JValue] match {
          case JNull => // If statement is still executing, result will be null.
            newPendingStatement()
            Future { waitForStatement(id) }
          case v: JValue => Future {v}
        }
        new Statement(id, null, result)
      }).toIndexedSeq
    }
  }

  override def stateChanged(oldState: SparkApp.State, newState: SparkApp.State): Unit = {
    // Error out the job if the app errors out.
    newState match {
      case SparkApp.State.FINISHED =>
        _state = SessionState.Success()
      case SparkApp.State.KILLED | SparkApp.State.FAILED =>
        _state = SessionState.Dead()
      case _ =>
    }
  }

  if (recovery) {
    if (url.isEmpty) {
      serverSideLog += "Unable to recover this session because livy-server crashed while " +
        "livy-repl was still starting."
      transition(SessionState.Error())
    } else {
      startStatementsRecovery()
    }
  }
}
