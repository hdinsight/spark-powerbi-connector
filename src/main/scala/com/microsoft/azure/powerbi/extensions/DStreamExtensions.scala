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

package com.microsoft.azure.powerbi.extensions

import java.lang.reflect.Field
import java.sql.Timestamp
import java.util.Date

import scala.collection.mutable.ListBuffer

import com.microsoft.azure.powerbi.authentication.PowerBIAuthentication
import com.microsoft.azure.powerbi.common.PowerBIUtils
import com.microsoft.azure.powerbi.models.{table, PowerBIDatasetDetails}

import org.apache.spark.streaming.dstream.DStream

object DStreamExtensions {

  implicit def PowerBIDStream[A](dStream: DStream[A]): PowerBIDStream[A]
  = new PowerBIDStream(dStream: DStream[A])

  class PowerBIDStream[A](dStream: DStream[A]) extends Serializable {

    def toPowerBI(powerbiDatasetDetails: PowerBIDatasetDetails, powerbiTable: table,
                  powerBIAuthentication: PowerBIAuthentication): Unit = {

      var authenticationToken: String = powerBIAuthentication.getAccessToken

      val powerbiTableColumnNames: List[String] = powerbiTable.columns.map(x => x.name)

      dStream.foreachRDD {
        rdd => {
          rdd.foreachPartition { partition =>

            // PowerBI row limit in single request is 10,000. We limit it to 1000.

            partition.grouped(1000).foreach {
              group => {
                val powerbiRowList: ListBuffer[Map[String, Any]] = ListBuffer[Map[String, Any]]()
                group.foreach {
                  record => {
                    powerbiRowList += (Map[String, Any]() /: record.getClass.getDeclaredFields) {
                      (objectFieldValueMap: Map[String, Any], objectField: Field) => {
                        objectField.setAccessible(true)

                        if (powerbiTableColumnNames.exists(x =>
                          x.equalsIgnoreCase(objectField.getName))) {
                          objectFieldValueMap + (objectField.getName -> objectField.get(record))
                        } else {
                          objectFieldValueMap
                        }
                      }
                    }
                  }

                  var attemptCount = 0
                  var pushSuccessful = false

                  while (!pushSuccessful && attemptCount < this.retryCount) {
                    try {
                      PowerBIUtils.addMultipleRows(powerbiDatasetDetails, powerbiTable,
                        powerbiRowList, authenticationToken)
                      pushSuccessful = true
                    }
                    catch {
                      case e: Exception => println("Exception inserting multiple rows: " +
                        e.getMessage)
                        Thread.sleep(secondsBetweenRetry * 1000)
                        attemptCount += 1

                        authenticationToken = powerBIAuthentication.refreshAccessToken
                    }
                  }
                }
              }
            }
          }
        }
      }
    }

    def countTimelineToPowerBI(powerbiDatasetDetails: PowerBIDatasetDetails, powerbiTable: table,
                               powerBIAuthentication: PowerBIAuthentication): Unit = {

      var authenticationToken: String = powerBIAuthentication.getAccessToken

      dStream.foreachRDD {
        rdd => {
          val currentTimestamp = new Timestamp(new Date().getTime)

          val powerbiRow = Map(powerbiTable.columns.head.name -> currentTimestamp,
            powerbiTable.columns(1).name -> rdd.count())

          var attemptCount = 0
          var pushSuccessful = false

          while (!pushSuccessful && attemptCount < this.retryCount) {
            try {
              PowerBIUtils.addRow(powerbiDatasetDetails, powerbiTable,
                powerbiRow, authenticationToken)
              pushSuccessful = true
            }
            catch {
              case e: Exception => println("Exception inserting row: " + e.getMessage)
                Thread.sleep(secondsBetweenRetry * 1000)
                attemptCount += 1

                authenticationToken = powerBIAuthentication.refreshAccessToken
            }
          }
        }
      }
    }

    private val retryCount: Int = 3
    private val secondsBetweenRetry: Int = 1
  }
}


