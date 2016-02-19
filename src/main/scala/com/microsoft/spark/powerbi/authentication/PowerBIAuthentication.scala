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

package com.microsoft.spark.powerbi.authentication

import java.util.concurrent.{ExecutorService, Executors, Future}
import javax.naming.ServiceUnavailableException

import com.microsoft.aad.adal4j.{AuthenticationContext, AuthenticationResult}

 case class PowerBIAuthentication(
                                  powerBIAuthorityURL: String,
                                  powerBIResourceURL: String,
                                  powerBIClientID: String,
                                  powerBIUsername: String,
                                  powerBIPassword: String
                                ) {

   def getAccessToken(): String = {

     getToken().getAccessToken()
  }

   def refreshAccessToken(): String = {

     getToken().getRefreshToken()
   }

   private def getToken(): AuthenticationResult ={

     var authenticationResult: AuthenticationResult = null

     var executorService: ExecutorService = null

     try {

       executorService = Executors.newFixedThreadPool(1)

       val authenticationContext: AuthenticationContext = new AuthenticationContext(powerBIAuthorityURL,
         true, executorService)

       val authenticationResultFuture: Future[AuthenticationResult] = authenticationContext.acquireToken(
         powerBIResourceURL, powerBIClientID, powerBIUsername, powerBIPassword, null)

       authenticationResult = authenticationResultFuture.get()
     }
     finally
     {
       executorService.shutdown()
     }

     if (authenticationResult == null) {

       throw new ServiceUnavailableException("Authentication result empty")
     }

     authenticationResult
   }
}
