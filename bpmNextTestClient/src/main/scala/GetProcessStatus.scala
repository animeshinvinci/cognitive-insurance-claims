package com.ibm.cicto

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import play.api.libs.json._
import play.api.libs.functional.syntax._
import play.api.libs.ws.WSAuthScheme
import play.api.libs.ws.WSResponse
import play.api.libs.ws.ning.NingWSClient

object GetProcessStatus extends App {

  override def main(args: Array[String]) {
    // Instantiation of the client
    // In a real-life application, you would instantiate one, share it everywhere,
    // and call wsClient.close() when you're done
    val wsClient = NingWSClient()
    val item = wsClient.url("https://wallaby.bpm.ibmcloud.com/bpm/dev/rest/bpm/wle/v1/processes/status/overview")
      .withAuth("ryan.claussen@gmail.com", "unsavory.rang.vulgar.maine", WSAuthScheme.BASIC)
      .withHeaders("Cache-Control" -> "no-cache")
      .withQueryString("searchFilter" -> "NEXT")
      .get()
      .map { wsResponse =>
        if (!(200 to 299).contains(wsResponse.status)) {
          sys.error(s"Received unexpected status ${wsResponse.status} : ${wsResponse.body}")
        }
        println((wsResponse.json \ "data" \ "overview").get)
        wsClient.close()
      }
  }
}