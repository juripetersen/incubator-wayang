/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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
package org.apache.wayang.api.json

import zio._
import zio.IO
import zio.http._
import zio.Console._
import scala.util.Try

import org.apache.wayang.api.json.builder.JsonPlanBuilder
import org.apache.wayang.api.json.operatorfromdrawflow.OperatorFromDrawflowConverter
import org.apache.wayang.api.json.operatorfromjson.OperatorFromJson
import org.apache.wayang.api.json.parserutil.ParseOperatorsFromDrawflow
import org.apache.wayang.api.json.parserutil.ParseOperatorsFromJson
import org.apache.wayang.api.json.operatorfromjson.OperatorFromJson

object Hello extends ZIOAppDefault {
  val drawRoute =
    Method.POST / "wayang-api-json" / "submit-plan" / "drawflow-format" -> handler { (req: Request) =>
     (for {
        requestBody <- req.body.asString
        operatorsFromDrawflow <- ZIO.fromTry(Try(ParseOperatorsFromDrawflow.parseOperatorsFromString(requestBody).get)) // Wrap with IO.fromTry to handle exceptions.
        operatorsFromJson = operatorsFromDrawflow.flatMap(op => OperatorFromDrawflowConverter.toOperatorFromJson(op)) // This should be a pure function, so it's fine to keep it outside of IO.
        result <- ZIO.attempt(new JsonPlanBuilder().setOperators(operatorsFromJson).execute())
        responseBody <- ZIO.attempt {
          if (operatorsFromJson.exists(op => op.cat == OperatorFromJson.Categories.Output))
            "Success"
          else
            result.collect().toString()
        }
        resBody <- ZIO.succeed(Response.text(responseBody))
     } yield resBody).orDie
  }

  val jsonRoute =
    Method.POST / "wayang-api-json" / "submit-plan" / "json" -> handler { (req: Request) =>
     (for {
        requestBody <- req.body.asString
        operatorsFromJson <- ZIO.fromTry(Try(ParseOperatorsFromJson.parseOperatorsFromString(requestBody).get))
        result <- ZIO.attempt(new JsonPlanBuilder().setOperators(operatorsFromJson).execute())
        responseBody <- ZIO.attempt {
          if (operatorsFromJson.exists(op => op.cat == OperatorFromJson.Categories.Output))
            "Success"
          else
            result.collect().toString()
        }
        resBody <- ZIO.succeed(Response.text(responseBody))
     } yield resBody).orDie
    }

  // Create HTTP route
  val app = Routes(drawRoute, jsonRoute).toHttpApp

  // Run it like any simple app
  override val run = Server.serve(app).provide(Server.default)
}