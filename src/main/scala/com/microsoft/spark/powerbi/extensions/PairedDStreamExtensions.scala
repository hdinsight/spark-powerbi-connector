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

import java.sql.Timestamp
import java.util.Date

import com.microsoft.spark.powerbi.authentication.PowerBIAuthentication
import com.microsoft.spark.powerbi.common.PowerBIUtils
import org.apache.spark.streaming.dstream.DStream

import com.microsoft.spark.powerbi.models.{table, PowerBIDatasetDetails}

import scala.reflect.runtime.{universe => runtimeUniverse}

object PairedDStreamExtensions {

  implicit def PowerBIPairedDStream[A, B: runtimeUniverse.TypeTag](dStream: DStream[(A, B)]): PowerBIPairedDStream[A, B]
  = new PowerBIPairedDStream(dStream: DStream[(A, B)])

  class PowerBIPairedDStream[A, B: runtimeUniverse.TypeTag](dStream: DStream[(A, B)]) extends Serializable {

    def stateTimelineToPowerBI(powerbiDatasetDetails: PowerBIDatasetDetails, powerbiTable: table,
                       powerBIAuthentication: PowerBIAuthentication): Unit = {

      dStream.foreachRDD(x => {

        if (x.count() > 0) {

          val currentTimestamp = new Timestamp(new Date().getTime())

          val powerbiRow = Map(powerbiTable.columns(0).name -> currentTimestamp,
            powerbiTable.columns(1).name -> x.first()._2)

          try {

            PowerBIUtils.addRow(powerbiDatasetDetails, powerbiTable, powerbiRow, powerBIAuthentication.getAccessToken())
          }
          catch {

            case e: Exception => println("Exception inserting row: " + e.getMessage())
          }
        }
      })
    }
  }
}


