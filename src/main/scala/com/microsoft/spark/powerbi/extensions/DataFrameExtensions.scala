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

package com.microsoft.spark.powerbi.extensions

import java.sql.{Timestamp}
import java.util.Date

import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.DataFrame

import com.microsoft.spark.powerbi.authentication.PowerBIAuthentication
import com.microsoft.spark.powerbi.models.{table, PowerBIDatasetDetails}
import com.microsoft.spark.powerbi.common.PowerBIUtils

object DataFrameExtensions {

  implicit def PowerBIDataFrame(dataFrame: DataFrame): PowerBIDataFrame = new PowerBIDataFrame(dataFrame: DataFrame)

  class PowerBIDataFrame(dataFrame: DataFrame) extends Serializable{

    def countTimelineToPowerBI(powerbiDatasetDetails: PowerBIDatasetDetails, powerbiTable: table,
                       powerBIAuthentication: PowerBIAuthentication): Unit = {

      val currentTimestamp = new Timestamp(new Date().getTime())

      val  powerbiRow = Map(powerbiTable.columns(0).name -> currentTimestamp,
        powerbiTable.columns(1).name -> dataFrame.count())

      try {

        PowerBIUtils.addRow(powerbiDatasetDetails, powerbiTable, powerbiRow, powerBIAuthentication.getAccessToken())
      }
      catch {

        case e: Exception => println("Exception inserting row: " + e.getMessage())
      }
    }

    def toPowerBI(powerbiDatasetDetails: PowerBIDatasetDetails, powerbiTable: table,
                  powerBIAuthentication: PowerBIAuthentication): Unit = {

      var authenticationToken: String = powerBIAuthentication.getAccessToken()

      dataFrame.foreachPartition { partition =>

        //PowerBI row limit in single request is 10,000. We limit it to 1000.

        partition.grouped(1000).foreach {

          group => {

            val powerbiRowListBuffer: ListBuffer[Map[String, Any]] = ListBuffer[Map[String, Any]]()

            group.foreach {

              record => {

                var powerbiRow: Map[String, Any] = Map[String, Any]()

                for (i <- 0 to record.length - 1) {

                  powerbiRow += (powerbiTable.columns(i).name -> record(i))
                }

                powerbiRowListBuffer += powerbiRow
              }

              try {

                PowerBIUtils.addMultipleRows(powerbiDatasetDetails, powerbiTable, powerbiRowListBuffer,
                  authenticationToken)
              }
              catch {

                case e: Exception => {

                  println(f"Exception inserting multiple rows: ${e.getMessage}")

                  authenticationToken = powerBIAuthentication.refreshAccessToken()
                }

              }
            }
          }
        }
      }
    }
  }
}