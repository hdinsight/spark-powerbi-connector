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

import java.lang.reflect.Field
import java.sql.Timestamp
import java.util.Date

import com.microsoft.spark.powerbi.authentication.PowerBIAuthentication
import com.microsoft.spark.powerbi.common.PowerBIUtils
import org.apache.spark.streaming.dstream.DStream

import com.microsoft.spark.powerbi.models.{table, PowerBIDatasetDetails}
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => runtimeUniverse}

object DStreamExtensions {

  implicit def PowerBIDStream[A: runtimeUniverse.TypeTag](dStream: DStream[A]): PowerBIDStream[A]
  = new PowerBIDStream(dStream: DStream[A])

  class PowerBIDStream[A: runtimeUniverse.TypeTag](dStream: DStream[A]) extends Serializable {

    def getFieldValueMap[A: runtimeUniverse.TypeTag](anyObject: A)
                                                    (implicit classTag: ClassTag[A]): Map[String, Any] = {

      var fieldValueMap: Map[String, Any] = Map[String, Any]()

      val objectFields: Array[Field] = anyObject.getClass().getDeclaredFields()

      objectFields.foreach(objectField => {

        val nameSymbol = runtimeUniverse.typeOf[A]
          .declaration(runtimeUniverse.stringToTermName(objectField.getName)).asTerm

        val typeMirror = runtimeUniverse.runtimeMirror(anyObject.getClass.getClassLoader)

        val instanceMirror = typeMirror.reflect[A](anyObject)(classTag)

        val fieldMirror = instanceMirror.reflectField(nameSymbol)

        fieldValueMap += (objectField.getName -> fieldMirror.get)

      })

      fieldValueMap
    }

    def countTimelineToPowerBI(powerbiDatasetDetails: PowerBIDatasetDetails, powerbiTable: table,
                       powerBIAuthentication: PowerBIAuthentication): Unit = {

      dStream.foreachRDD {

        rdd => {

          val currentTimestamp = new Timestamp(new Date().getTime())

          val powerbiRow = Map(powerbiTable.columns(0).name -> currentTimestamp,
            powerbiTable.columns(1).name -> rdd.count())

          try {

            PowerBIUtils.addRow(powerbiDatasetDetails, powerbiTable, powerbiRow, powerBIAuthentication.getAccessToken())
          }
          catch {

            case e: Exception => println("Exception inserting row: " + e.getMessage())
          }
        }
      }
    }

    def toPowerBI(powerbiDatasetDetails: PowerBIDatasetDetails, powerbiTable: table,
                  powerBIAuthentication: PowerBIAuthentication)(implicit classTag: ClassTag[A]): Unit = {

      var authenticationToken: String = powerBIAuthentication.getAccessToken()

      dStream.foreachRDD {

        rdd => {

          rdd.foreachPartition { partition =>

            //PowerBI row limit in single request is 10,000. We limit it to 1000.

            partition.grouped(1000).foreach {

              group => {

                val powerbiRowList: ListBuffer[Map[String, Any]] = ListBuffer[Map[String, Any]]()

                group.foreach {

                  record => {

                    powerbiRowList += getFieldValueMap(record)
                  }

                  try {

                    PowerBIUtils.addMultipleRows(powerbiDatasetDetails, powerbiTable, powerbiRowList,
                      authenticationToken)
                  }
                  catch {

                    case e: Exception => {

                      println("Exception inserting multiple rows: " + e.getMessage())

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
  }
}


