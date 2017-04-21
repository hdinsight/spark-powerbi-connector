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

package com.microsoft.azure.powerbi.authentication

import java.net.URI
import java.util.concurrent.{Executors, ExecutorService, Future}
import javax.naming.ServiceUnavailableException

import com.microsoft.aad.adal4j.{AuthenticationContext, AuthenticationResult}

case class PowerBIAuthenticationWithUsernamePassword(powerBIAuthorityURL: String,
                                                     powerBIResourceURL: String,
                                                     powerBIClientID: String,
                                                     activeDirectoryUsername: String,
                                                     activeDirectoryPassword: String)
  extends PowerBIAuthentication{

  def getAccessToken: String =
    if (this.accessToken != null && this.accessToken.nonEmpty) this.accessToken
    else refreshAccessToken

  def refreshAccessToken: String = retrieveToken.getAccessToken

  private def retrieveToken: AuthenticationResult = {

    var authenticationResult: AuthenticationResult = null
    var executorService: ExecutorService = null

    try {

      executorService = Executors.newFixedThreadPool(1)

      val authenticationContext: AuthenticationContext =
        new AuthenticationContext(powerBIAuthorityURL, true, executorService)

      val authenticationResultFuture: Future[AuthenticationResult] =
        authenticationContext.acquireToken(powerBIResourceURL, powerBIClientID,
          activeDirectoryUsername, activeDirectoryPassword, null)

      authenticationResult = authenticationResultFuture.get()
    }
    finally
    {
      executorService.shutdown()
    }

    if (authenticationResult == null) {
      throw new ServiceUnavailableException("Authentication result empty")
    }

    this.accessToken = authenticationResult.getAccessToken

    authenticationResult
  }

  private var accessToken: String = _
}