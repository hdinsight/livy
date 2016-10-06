/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.livy.rsc.driver;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.json4s.JsonAST.JObject;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Statement {
  public static class Result {
    public static String STATUS_OK = "ok";
    public static String STATUS_ERROR = "error";

    public final String status;
    @JsonProperty("execution_count")
    public final Integer executionCount;

    private Result(String status, Integer executionCount) {
      this.status = status;
      this.executionCount = executionCount;

    }

    // For deserialization.
    public Result() {
      this(null, null);
    }
  }

  public static class OkResult extends Result {
    public final JObject data;

    public OkResult(Integer executionCount, JObject data) {
      super(STATUS_OK, executionCount);
      this.data = data;
    }

    public OkResult() {
      this(null, null);
    }
  }

  public static class ErrorResult extends Result {
    public final String ename;
    public final String evalue;
    public final String[] traceback;

    public ErrorResult(Integer executionCount, String ename, String evalue, String[] traceback)
    {
      super(STATUS_ERROR, executionCount);
      this.ename = ename;
      this.evalue = evalue;
      this.traceback = traceback;
    }

    public ErrorResult() {
      this(null, null, null, null);
    }
  }

  public final int id;
  public final StatementState state;
  public final Result output;

  public Statement(int id, StatementState state, Result output) {
    this.id = id;
    this.state = state;
    this.output = output;
  }
}
