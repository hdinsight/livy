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

import org.json4s.{DefaultFormats, JValue}
import org.json4s.JsonDSL._
import org.scalatest.Inside.inside

import com.cloudera.livy.repl
import com.cloudera.livy.repl.Interpreter._
import com.cloudera.livy.repl.python.PythonInterpreter

class PythonInterpreterSpec extends BaseInterpreterSpec {
  implicit val formats = DefaultFormats

  private def verifyError(
    response: ExecuteResponse,
    ename: String,
    evalue: String,
    tracebackPattern: Option[String] = None) = {
    inside(response) {
      case Interpreter.ExecuteError(actualEname, actualEvalue, actualTraceback) =>
        actualEname should equal (ename)
        actualEvalue should equal (evalue)
        val actualTracebackLines = actualTraceback.mkString("")
        tracebackPattern.foreach(actualTracebackLines should include regex _)
    }
  }

  override def createInterpreter(): Interpreter = PythonInterpreter()

  it should "execute `1 + 2` == 3" in withInterpreter { interpreter =>
    val response = interpreter.execute("1 + 2")
    response should equal (Interpreter.ExecuteSuccess(
      repl.TEXT_PLAIN -> "3"
    ))
  }

  it should "execute multiple statements" in withInterpreter { interpreter =>
    var response = interpreter.execute("x = 1")
    response should equal (Interpreter.ExecuteSuccess(
      repl.TEXT_PLAIN -> ""
    ))

    response = interpreter.execute("y = 2")
    response should equal (Interpreter.ExecuteSuccess(
      repl.TEXT_PLAIN -> ""
    ))

    response = interpreter.execute("x + y")
    response should equal (Interpreter.ExecuteSuccess(
      repl.TEXT_PLAIN -> "3"
    ))
  }

  it should "execute multiple statements in one block" in withInterpreter { interpreter =>
    val response = interpreter.execute(
      """
        |x = 1
        |
        |y = 2
        |
        |x + y
      """.stripMargin)
    response should equal(Interpreter.ExecuteSuccess(
      repl.TEXT_PLAIN -> "3"
    ))
  }

  it should "parse a class" in withInterpreter { interpreter =>
    val response = interpreter.execute(
      """
        |class Counter(object):
        |   def __init__(self):
        |       self.count = 0
        |
        |   def add_one(self):
        |       self.count += 1
        |
        |   def add_two(self):
        |       self.count += 2
        |
        |counter = Counter()
        |counter.add_one()
        |counter.add_two()
        |counter.count
      """.stripMargin)
    response should equal(Interpreter.ExecuteSuccess(
      repl.TEXT_PLAIN -> "3"
    ))
  }

  it should "do json magic" in withInterpreter { interpreter =>
    val response = interpreter.execute(
      """x = [[1, 'a'], [3, 'b']]
        |%json x
      """.stripMargin)

    response should equal(Interpreter.ExecuteSuccess(
      repl.APPLICATION_JSON -> List[JValue](
        List[JValue](1, "a"),
        List[JValue](3, "b")
      )
    ))
  }

  it should "do table magic" in withInterpreter { interpreter =>
    val response = interpreter.execute(
      """x = [[1, 'a'], [3, 'b']]
        |%table x
      """.stripMargin)

    response should equal(Interpreter.ExecuteSuccess(
      repl.APPLICATION_LIVY_TABLE_JSON -> (
        ("headers" -> List(
          ("type" -> "INT_TYPE") ~ ("name" -> "0"),
          ("type" -> "STRING_TYPE") ~ ("name" -> "1")
        )) ~
          ("data" -> List(
            List[JValue](1, "a"),
            List[JValue](3, "b")
          ))
        )
    ))
  }

  it should "allow magic inside statements" in withInterpreter { interpreter =>
    val response = interpreter.execute(
      """x = [[1, 'a'], [3, 'b']]
        |%table x
        |1 + 2
      """.stripMargin)

    response should equal(Interpreter.ExecuteSuccess(
      repl.TEXT_PLAIN -> "3"
    ))
  }

  it should "capture stdout" in withInterpreter { interpreter =>
    val response = interpreter.execute("print 'Hello World'")
    response should equal(Interpreter.ExecuteSuccess(
      repl.TEXT_PLAIN -> "Hello World"
    ))
  }

  it should "report an error if accessing an unknown variable" in withInterpreter { interpreter =>
    val response = interpreter.execute("x")
    verifyError(response, "NameError", "name 'x' is not defined")
  }

  it should "report an error if empty magic command" in withInterpreter { interpreter =>
    val response = interpreter.execute("%")
    verifyError(response, "UnknownMagic", "magic command not specified")
  }

  it should "report an error if unknown magic command" in withInterpreter { interpreter =>
    val response = interpreter.execute("%foo")
    verifyError(response, "UnknownMagic", "unknown magic command 'foo'")
  }

  it should "not execute part of the block if there is a syntax error" in withInterpreter { intp =>
    var response = intp.execute(
      """x = 1
        |'
      """.stripMargin)
    verifyError(
      response,
      "SyntaxError", "EOL while scanning string literal (<stdin>, line 2)",
      Some("(?s)File \"<stdin>\", line 2.*SyntaxError"))

    response = intp.execute("x")
    verifyError(
      response,
      "NameError", "name 'x' is not defined",
      // Pattern line.*in makes sure the traceback has the full stacktrace.
      Some("(?s)Traceback.*line.*in.*NameError"))
  }

  // Scalastyle is treating unicode escape as non ascii characters. Turn off the check.
  // scalastyle:off non.ascii.character.disallowed
  it should "print unicode correctly" in withInterpreter { intp =>
    intp.execute("print(u\"\u263A\")") should equal(Interpreter.ExecuteSuccess(
      TEXT_PLAIN -> "\u263A"
    ))
    intp.execute("""print(u"\u263A")""") should equal(Interpreter.ExecuteSuccess(
      TEXT_PLAIN -> "\u263A"
    ))
    intp.execute("""print("\xE2\x98\xBA")""") should equal(Interpreter.ExecuteSuccess(
      TEXT_PLAIN -> "\u263A"
    ))
  }
  // scalastyle:on non.ascii.character.disallowed
}
