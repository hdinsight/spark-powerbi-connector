package com.microsoft.spark.powerbi.exceptions

case class PowerBIClientException(statusCode: Int, responseMessage: String, exceptionMessage: String)
  extends Exception(f"Status Code: $statusCode, Response Message: $responseMessage," +
    f" Exception Message: $exceptionMessage") {
}