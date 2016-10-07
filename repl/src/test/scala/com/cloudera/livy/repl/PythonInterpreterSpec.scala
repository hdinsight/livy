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

import com.google.common.collect.ImmutableMap
import org.apache.spark.SparkConf
import org.scalatest._

import com.cloudera.livy.sessions._

abstract class PythonBaseInterpreterSpec extends BaseInterpreterSpec {

  it should "execute `1 + 2` == 3" in withInterpreter { interpreter =>
    val response = interpreter.execute("1 + 2")
    response shouldBe Interpreter.ExecuteSuccess(ImmutableMap.of(TEXT_PLAIN, "3"))
  }

  it should "execute multiple statements" in withInterpreter { interpreter =>
    var response = interpreter.execute("x = 1")
    response shouldBe Interpreter.ExecuteSuccess(ImmutableMap.of(TEXT_PLAIN, ""))

    response = interpreter.execute("y = 2")
    response shouldBe Interpreter.ExecuteSuccess(ImmutableMap.of(TEXT_PLAIN, ""))

    response = interpreter.execute("x + y")
    response shouldBe Interpreter.ExecuteSuccess(ImmutableMap.of(TEXT_PLAIN, "3"))
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
    response shouldBe Interpreter.ExecuteSuccess(ImmutableMap.of(TEXT_PLAIN, "3"))
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
    response shouldBe Interpreter.ExecuteSuccess(ImmutableMap.of(TEXT_PLAIN, "3"))
  }

  it should "do json magic" in withInterpreter { interpreter =>
    val response = interpreter.execute(
      """x = [[1, 'a'], [3, 'b']]
        |%json x
      """.stripMargin)

    response shouldBe Interpreter.ExecuteSuccess(ImmutableMap.of(
      APPLICATION_JSON, """[[1, "a"], [3, "b"]]"""))
  }

  /*
  it should "do table magic" in withInterpreter { interpreter =>
    val response = interpreter.execute(
      """x = [[1, 'a'], [3, 'b']]
        |%table x
      """.stripMargin)

    response shouldBe Interpreter.ExecuteSuccess(ImmutableMap.of(
      APPLICATION_LIVY_TABLE_JSON -> (
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
  }*/

  it should "allow magic inside statements" in withInterpreter { interpreter =>
    val response = interpreter.execute(
      """x = [[1, 'a'], [3, 'b']]
        |%table x
        |1 + 2
      """.stripMargin)

    response shouldBe Interpreter.ExecuteSuccess(ImmutableMap.of(TEXT_PLAIN, "3"))
  }

  it should "capture stdout" in withInterpreter { interpreter =>
    val response = interpreter.execute("print('Hello World')")
    response shouldBe Interpreter.ExecuteSuccess(ImmutableMap.of(TEXT_PLAIN, "Hello World"))
  }

  it should "report an error if accessing an unknown variable" in withInterpreter { interpreter =>
    val response = interpreter.execute("x")
    response shouldBe Interpreter.ExecuteError(
      "NameError",
      "name 'x' is not defined",
      List(
        "Traceback (most recent call last):\n",
        "NameError: name 'x' is not defined\n"
      )
    )
  }

  it should "report an error if empty magic command" in withInterpreter { interpreter =>
    val response = interpreter.execute("%")
    response shouldBe Interpreter.ExecuteError(
      "UnknownMagic",
      "magic command not specified",
      List("UnknownMagic: magic command not specified\n")
    )
  }

  it should "report an error if unknown magic command" in withInterpreter { interpreter =>
    val response = interpreter.execute("%foo")
    response shouldBe Interpreter.ExecuteError(
      "UnknownMagic",
      "unknown magic command 'foo'",
      List("UnknownMagic: unknown magic command 'foo'\n")
    )
  }

  it should "not execute part of the block if there is a syntax error" in withInterpreter { intp =>
    var response = intp.execute(
      """x = 1
        |'
      """.stripMargin)

    response shouldBe Interpreter.ExecuteError(
      "SyntaxError",
      "EOL while scanning string literal (<stdin>, line 2)",
      List(
        "  File \"<stdin>\", line 2\n",
        "    '\n",
        "    ^\n",
        "SyntaxError: EOL while scanning string literal\n"
      )
    )

    response = intp.execute("x")
    response shouldBe Interpreter.ExecuteError(
      "NameError",
      "name 'x' is not defined",
      List(
        "Traceback (most recent call last):\n",
        "NameError: name 'x' is not defined\n"
      )
    )
  }
}

class Python2InterpreterSpec extends PythonBaseInterpreterSpec {
  override def createInterpreter(): Interpreter = PythonInterpreter(new SparkConf(), PySpark())

  // Scalastyle is treating unicode escape as non ascii characters. Turn off the check.
  // scalastyle:off non.ascii.character.disallowed
  it should "print unicode correctly" in withInterpreter { intp =>
    intp.execute("print(u\"\u263A\")") shouldBe Interpreter.ExecuteSuccess(ImmutableMap.of(
      TEXT_PLAIN, "\u263A"
    ))
    intp.execute("""print(u"\u263A")""") shouldBe Interpreter.ExecuteSuccess(ImmutableMap.of(
      TEXT_PLAIN, "\u263A"
    ))
    intp.execute("""print("\xE2\x98\xBA")""") shouldBe Interpreter.ExecuteSuccess(ImmutableMap.of(
      TEXT_PLAIN, "\u263A"
    ))
  }
  // scalastyle:on non.ascii.character.disallowed
}

class Python3InterpreterSpec extends PythonBaseInterpreterSpec {

  override protected def withFixture(test: NoArgTest): Outcome = {
    assume(!sys.props.getOrElse("skipPySpark3Tests", "false").toBoolean, "Skipping PySpark3 tests.")
    test()
  }

  override def createInterpreter(): Interpreter = PythonInterpreter(new SparkConf(), PySpark3())

  it should "check python version is 3.x" in withInterpreter { interpreter =>
    val response = interpreter.execute("""import sys
      |sys.version >= '3'
      """.stripMargin)
    response shouldBe Interpreter.ExecuteSuccess(ImmutableMap.of(TEXT_PLAIN, "True"))
  }
}
