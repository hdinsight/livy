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
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.SparkConf

class SparkSessionSpec extends BaseSessionSpec {

  override def createInterpreter(): Interpreter = new SparkInterpreter(new SparkConf())

  it should "execute `1 + 2` == 3" in withSession { session =>
    verifyOkResult(execute(session, "1 + 2"), 0)
    /*
    val statement = execute(session, "1 + 2")
    statement.id should equal (0)

    val result = statement.output
    val expectedResult = Extraction.decompose(Map(
      "status" -> "ok",
      "execution_count" -> 0,
      "data" -> Map(
        "text/plain" -> "res0: Int = 3"
      )
    ))

    result should equal (expectedResult)*/
  }

  it should "execute `x = 1`, then `y = 2`, then `x + y`" in withSession { session =>
    val statementId = new AtomicInteger()

    verifyOkResult(execute(session, "val x = 1"), statementId.getAndIncrement())
    verifyOkResult(execute(session, "val y = 2"), statementId.getAndIncrement())
    verifyOkResult(execute(session, "x + y"), statementId.getAndIncrement())
    /*
    var statement = execute(session, "val x = 1")
    statement.id should equal (0)

    var result = statement.output
    var expectedResult = Extraction.decompose(Map(
      "status" -> "ok",
      "execution_count" -> 0,
      "data" -> Map(
        "text/plain" -> "x: Int = 1"
      )
    ))

    result should equal (expectedResult)

    statement = execute(session, "val y = 2")
    statement.id should equal (1)

    result = statement.output
    expectedResult = Extraction.decompose(Map(
      "status" -> "ok",
      "execution_count" -> 1,
      "data" -> Map(
        "text/plain" -> "y: Int = 2"
      )
    ))

    result should equal (expectedResult)

    statement = execute(session, "x + y")
    statement.id should equal (2)

    result = statement.output
    expectedResult = Extraction.decompose(Map(
      "status" -> "ok",
      "execution_count" -> 2,
      "data" -> Map(
        "text/plain" -> "res0: Int = 3"
      )
    ))

    result should equal (expectedResult)*/
  }

  it should "capture stdout" in withSession { session =>
    verifyOkResult(execute(session, """println("Hello World")"""), 0)

    /*
    val statement = execute(session, """println("Hello World")""")
    statement.id should equal (0)

    val result = statement.output
    val expectedResult = Extraction.decompose(Map(
      "status" -> "ok",
      "execution_count" -> 0,
      "data" -> Map(
        "text/plain" -> "Hello World"
      )
    ))

    result should equal (expectedResult)
    */
  }

  it should "report an error if accessing an unknown variable" in withSession { session =>
    verifyErrorResult(execute(session, "x"), 0)
    /*
    val statement = execute(session, """x""")
    statement.id should equal (0)

    val result = statement.output

    result.status should equal ("error")
    result.executionCount should equal ("0")
    result.ename should equal ("Error")
    result.evalue should include ("error: not found: value x")
    */
  }

  it should "report an error if exception is thrown" in withSession { session =>
    val code =
      """def func1() {
        |throw new Exception()
        |}
        |func1()""".stripMargin
    verifyErrorResult(execute(session, code), 0)
    /*
    val statement = execute(session,
      )
    statement.id should equal (0)

    val result = statement.output

    result.status should equal ("error")
    result.executionCount should equal (0)
    result.ename should equal ("Error")
    result.evalue should include ("java.lang.Exception")

    val traceback = result.traceback
    traceback should not be empty
    traceback.get(0) should include ("func1(<console>:")
    */
  }

  it should "execute spark commands" in withSession { session =>
    verifyOkResult(execute(session, "sc.parallelize(0 to 1).map{i => i+1}.collect"), 0)

    /*
    val statement = execute(session,
      """".stripMargin)
    statement.id should equal (0)

    val result = statement.output

    val expectedResult = Extraction.decompose(Map(
      "status" -> "ok",
      "execution_count" -> 0,
      "data" -> Map(
        "text/plain" -> "res0: Array[Int] = Array(1, 2)"
      )
    ))

    result should equal (expectedResult)
    */
  }

  it should "do table magic" in withSession { session =>
    verifyOkResult(execute(session, "val x = List((1, \"a\"), (3, \"b\"))\n%table x"), 0)
    /*
    val statement = execute(session, "val x = List((1, \"a\"), (3, \"b\"))\n%table x")
    statement.id should equal (0)

    val result = statement.output

    val expectedResult = Extraction.decompose(Map(
      "status" -> "ok",
      "execution_count" -> 0,
      "data" -> Map(
        "application/vnd.livy.table.v1+json" -> Map(
          "headers" -> List(
            Map("type" -> "BIGINT_TYPE", "name" -> "_1"),
            Map("type" -> "STRING_TYPE", "name" -> "_2")),
          "data" -> List(List(1, "a"), List(3, "b"))
        )
      )
    ))

    result should equal (expectedResult)
    */
  }
}
