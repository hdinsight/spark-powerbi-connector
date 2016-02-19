package com.microsoft.spark.powerbi.exceptions

case class PowerBIClientException(statusCode: Int, responseMessage: String, exceptionMessage: String)
  extends Exception(exceptionMessage)