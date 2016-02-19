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

package com.microsoft.spark.powerbi.clients

import org.json4s.ShortTypeHints
import org.json4s.native.Serialization
import org.json4s.native.Serialization._
import org.apache.http.client.methods._

import com.microsoft.spark.powerbi.models._
import com.microsoft.spark.powerbi.common._
import com.microsoft.spark.powerbi.exceptions._

import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}

object PowerBITileClient {

  def get(dashboardId: String, authenticationToken: String, groupId: String = null): PowerBITileDetailsList = {

    implicit val formats = Serialization.formats(
      ShortTypeHints(
        List()
      )
    )

    var getRequestURL: String = null

    if(groupId == null || groupId.trim.isEmpty) {

      getRequestURL = PowerBIURLs.DashboardsBeta + s"/dashboards/$dashboardId/tiles"

    } else {

      getRequestURL = PowerBIURLs.GroupsBeta + f"/$groupId/dashboards/$dashboardId/tiles"
    }

    val getRequest: HttpGet = new HttpGet(getRequestURL)

    getRequest.addHeader("Authorization", f"Bearer $authenticationToken")

    val httpClient: CloseableHttpClient = HttpClientUtils.getCustomHttpClient()

    var responseContent: String = null
    var statusCode: Int = -1
    var exceptionMessage: String = null

    try {

      val httpResponse = httpClient.execute(getRequest)
      statusCode = httpResponse.getStatusLine().getStatusCode()

      val responseEntity = httpResponse.getEntity()

      if (responseEntity != null) {

        val inputStream = responseEntity.getContent()
        responseContent = scala.io.Source.fromInputStream(inputStream).getLines.mkString
        inputStream.close
      }
    }
    catch {

      case e: Exception => exceptionMessage = e.getMessage
    }
    finally {

      httpClient.close()
    }

    if (statusCode == 200) {

      return read[PowerBITileDetailsList](responseContent)
    }

    throw new PowerBIClientException(statusCode, responseContent, exceptionMessage)
  }
}