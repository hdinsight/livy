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

class SparkRSessionSpec extends BaseSessionSpec {

  override protected def withFixture(test: NoArgTest) = {
    assume(!sys.props.getOrElse("skipRTests", "false").toBoolean, "Skipping R tests.")
    super.withFixture(test)
  }

  override def createInterpreter(): Interpreter = SparkRInterpreter(new SparkConf())

  it should "execute `1 + 2` == 3" in withSession { session =>
    verifyOkResult(execute(session, "1 + 2"), 0)

    // TODO UFO
    /*
    val result = statement.output
    result.status shouldBe STATUS_OK
    result.executionCount shouldBe 0

    val expectedResult = Extraction.decompose(Map(
      "data" -> Map(
        "text/plain" -> "[1] 3"
      )
    ))
    */
  }

  it should "execute `x = 1`, then `y = 2`, then `x + y`" in withSession { session =>
    val statementId = new AtomicInteger()

    verifyOkResult(execute(session, "x = 1"), statementId.getAndIncrement())
    // TODO UFO
    /*
    var expectedResult = Extraction.decompose(Map(
      "status" -> "ok",
      "execution_count" -> 0,
      "data" -> Map(
        "text/plain" -> ""
      )
    ))*/

    verifyOkResult(execute(session, "y = 2"), statementId.getAndIncrement())
    /* TODO UFO
    expectedResult = Extraction.decompose(Map(
      "status" -> "ok",
      "execution_count" -> 1,
      "data" -> Map(
        "text/plain" -> ""
      )
    ))
    */

    verifyOkResult(execute(session, "x + y"), statementId.getAndIncrement())
    /*
    result = statement.output
    expectedResult = Extraction.decompose(Map(
      "status" -> "ok",
      "execution_count" -> 2,
      "data" -> Map(
        "text/plain" -> "[1] 3"
      )
    ))

    result should equal (expectedResult)*/
  }

  it should "capture stdout from print" in withSession { session =>
    verifyOkResult(execute(session, "print('Hello World')"), 0)

    /*
    val result = statement.output
    val expectedResult = Extraction.decompose(Map(
      "status" -> "ok",
      "execution_count" -> 0,
      "data" -> Map(
        "text/plain" -> "[1] \"Hello World\""
      )
    ))

    result should equal (expectedResult)
    */
  }

  it should "capture stdout from cat" in withSession { session =>
    verifyOkResult(execute(session, "cat(3)"), 0)

    /*
    val statement = execute(session, """cat(3)""")
    statement.id should equal (0)

    val result = statement.output
    val expectedResult = Extraction.decompose(Map(
      "status" -> "ok",
      "execution_count" -> 0,
      "data" -> Map(
        "text/plain" -> "3"
      )
    ))

    result should equal (expectedResult)
    */
  }

  it should "report an error if accessing an unknown variable" in withSession { session =>
    verifyOkResult(execute(session, "x"), 0)

    /*
    val result = statement.output
    val expectedResult = Extraction.decompose(Map(
      "status" -> "ok",
      "execution_count" -> 0,
      "data" -> Map(
        "text/plain" -> "Error: object 'x' not found"
      )
    ))

    result should equal (expectedResult)
    */
  }

}
