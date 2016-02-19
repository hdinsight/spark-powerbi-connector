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

package com.microsoft.spark.powerbi.common

import com.microsoft.spark.powerbi.clients._
import com.microsoft.spark.powerbi.models._
import com.microsoft.spark.powerbi.exceptions._

import scala.collection.mutable.ListBuffer

object PowerBIUtils {

  def defineTable(tableName: String, columnNameTypeMap: Map[String, String]): table = {

    val powerbiColumns : ListBuffer[column] = ListBuffer[column]()

    columnNameTypeMap.foreach(x => powerbiColumns += column(x._1, x._2))

    table(tableName, powerbiColumns.toList)
  }

  def createDataset(powerbiDatasetName: String, powerbiTables: List[table],
                    retentionPolicy: PowerBIOptions.DatasetRetentionPolicy = PowerBIOptions.basicFIFO,
                    authenticationToken: String, groupId: String = null): PowerBIDatasetDetails = {

    val powerbiDataset: PowerBIDataset = PowerBIDataset(powerbiDatasetName, powerbiTables)

    val powerbiDatasetDetails: PowerBIDatasetDetails = PowerBIDatasetClient.create(powerbiDataset, retentionPolicy,
      authenticationToken, groupId)

    println("Id: " + powerbiDatasetDetails.id + " Name: " + powerbiDatasetDetails.name)

    powerbiDatasetDetails
  }

  def getDataset(powerbiDatasetName: String, authenticationToken: String, groupId: String = null)
  : PowerBIDatasetDetails = {

    val powerbiDatasetDetailsList: PowerBIDatasetDetailsList = PowerBIDatasetClient.get(authenticationToken, groupId)

    powerbiDatasetDetailsList.value.find(x => x.name.equalsIgnoreCase(powerbiDatasetName)).getOrElse(null)
  }


  def getOrCreateDataset(powerbiDatasetName: String, powerbiTables: List[table],
                         retentionPolicy: PowerBIOptions.DatasetRetentionPolicy = PowerBIOptions.basicFIFO,
                         authenticationToken: String, groupId: String = null): PowerBIDatasetDetails = {


    val powerbiDatasetDetails: PowerBIDatasetDetails = getDataset(powerbiDatasetName, authenticationToken, groupId)

    //Check for existing tables by name, schema is not checked

    if (powerbiDatasetDetails != null) {

      val powerbiTableDetailsList: PowerBITableDetailsList = PowerBITableClient.get(powerbiDatasetDetails.id,
        authenticationToken, groupId)

      powerbiTables.foreach(powerbiTable => {

        println("Examining table: " + powerbiTable.name)

        val powerbiTableDetails  = powerbiTableDetailsList.value.find(x => x.name.equalsIgnoreCase(powerbiTable.name))
          .getOrElse(null)

        if (powerbiTableDetails == null) {

          val exceptionMessage = powerbiTable.name + " not found in dataset " + powerbiDatasetDetails.name

          throw new PowerBIClientException(-1, null, exceptionMessage)
        }
      })

      return powerbiDatasetDetails
    }

    createDataset(powerbiDatasetName, powerbiTables, retentionPolicy, authenticationToken, groupId)
  }

  def addRow(powerbiDatasetDetails: PowerBIDatasetDetails,  powerbiTable: table, columnNameValueMap: Map[String, Any],
             authenticationToken: String, groupId: String = null): String = {

    val powerbiRows: ListBuffer[Map[String, Any]] = ListBuffer[Map[String, Any]]()

    powerbiRows += columnNameValueMap

    PowerBIRowClient.add(PowerBIRows(powerbiRows.toList), powerbiTable.name, powerbiDatasetDetails.id,
      authenticationToken, null)
  }

  def addMultipleRows(powerbiDatasetDetails: PowerBIDatasetDetails, powerbiTable: table,
                      columnNameValueMapList: List[Map[String, Any]], authenticationToken: String,
                      groupId: String = null): String = {

    PowerBIRowClient.add(PowerBIRows(columnNameValueMapList), powerbiTable.name, powerbiDatasetDetails.id,
      authenticationToken, null)
  }

}