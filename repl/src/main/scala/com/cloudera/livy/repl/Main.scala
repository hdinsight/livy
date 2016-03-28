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

package com.cloudera.livy.repl

import java.util.concurrent.TimeUnit
import javax.servlet.ServletContext

import scala.annotation.tailrec
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

import dispatch._
import org.eclipse.jetty.servlet.ServletHolder
import org.json4s.{DefaultFormats, Formats}
import org.json4s.jackson.Serialization.write
import org.scalatra.LifeCycle
import org.scalatra.servlet.ScalatraListener

import com.cloudera.livy.{LivyConf, Logging, WebServer}
import com.cloudera.livy.repl.python.PythonInterpreter
import com.cloudera.livy.repl.scalaRepl.SparkInterpreter
import com.cloudera.livy.repl.sparkr.SparkRInterpreter
import com.cloudera.livy.sessions.SessionState

// scalastyle:off println
object Main extends Logging {
  private implicit def executor: ExecutionContext = ExecutionContext.global
  private implicit def jsonFormats: Formats = DefaultFormats

  def main(args: Array[String]): Unit = {
    val host = sys.props.getOrElse("spark.livy.host", "0.0.0.0")
    val port = sys.props.getOrElse("spark.livy.port", "8999").toInt
    val callbackUrl = sys.props.get("spark.livy.callbackUrl")

    require(args.length >= 1, "Must specify either `pyspark`/`spark`/`sparkr` for the session kind")

    val session_kind = args.head

    val server = new WebServer(new LivyConf(), host, port)
    val session = createSession(session_kind)
    server.context.addServlet(new ServletHolder(new WebApp(session)), "/*")

    try {
      server.start()

      val replUrl = s"http://${server.host}:${server.port}"
      println(s"Starting livy-repl on $replUrl")
      Console.flush()

      callbackUrl.foreach(notifyCallback(session, replUrl, _))

      server.join()
    } finally {
      server.stop()
      // Make sure to close all our outstanding http requests.
      Http.shutdown()
    }
  }


  private def createSession(session_kind: String): Session = {
    val interpreter = session_kind match {
      case "pyspark" => PythonInterpreter()
      case "spark" => SparkInterpreter()
      case "sparkr" => SparkRInterpreter()
      case _ => throw new Exception("Unknown session kind: " + session_kind)
    }
    Session(interpreter)
  }

  private def notifyCallback(session: Session, replUrl: String, callbackUrl: String): Unit = {
    info(s"Notifying $callbackUrl that we're up")

    session.waitForStateChange(SessionState.Starting(), Duration(1, TimeUnit.MINUTES))

    var req = url(callbackUrl).setContentType("application/json", "UTF-8")
    req = req << write(Map("url" -> replUrl))

    info(s"Calling $callbackUrl...")
    val rep = Http(req OK as.String)
    rep.onFailure { case t: Throwable => throw t }

    Await.result(rep, Duration(10, TimeUnit.SECONDS))
  }
}
// scalastyle:on println
