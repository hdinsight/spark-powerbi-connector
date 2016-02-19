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

package com.microsoft.spark.powerbi.models

case class PowerBIDatasetDetails(id: String, name: String)
case class PowerBIDatasetDetailsList(value: List[PowerBIDatasetDetails])

case class PowerBITableDetails(name: String)
case class PowerBITableDetailsList(value: List[PowerBITableDetails])

case class PowerBIGroupDetails(id: String, name: String)
case class PowerBIGroupDetailsList(value: List[PowerBIGroupDetails])

case class PowerBIDashboardDetails(id: String, displayName: String)
case class PowerBIDashboardDetailsList(value: List[PowerBIDashboardDetails])

case class PowerBITileDetails(id: String, title: String, embedUrl: String)
case class PowerBITileDetailsList(value: List[PowerBITileDetails])

case class PowerBIReportDetails(id: String, name: String, webUrl: String, embedUrl: String)
case class PowerBIReportDetailsList(value: List[PowerBIReportDetails])
