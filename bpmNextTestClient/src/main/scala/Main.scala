package com.ibm.cicto

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import play.api.libs.ws.WSAuthScheme
import play.api.libs.ws.WSResponse
import play.api.libs.ws.ning.NingWSClient
import java.io._
import play.api.libs.json._

object CallCognitiveClaim extends App {

  override def main(args: Array[String]) {
    // Instantiation of the client
    // In a real-life application, you would instantiate one, share it everywhere,
    // and call wsClient.close() when you're done
    val wsClient = NingWSClient()
    var a = 0
    var calls: List[Future[WSResponse]] = List()
    for (a <- 1 to 500) {
      val item = wsClient.url("https://wallaby.bpm.ibmcloud.com/bpm/dev/rest/bpm/wle/v1/process")
        //.withQueryString("some_parameter" -> "some_value", "some_other_parameter" -> "some_other_value")
        .withAuth("ryan.claussen@gmail.com", "unsavory.rang.vulgar.maine", WSAuthScheme.BASIC)
        .withHeaders("Cache-Control" -> "no-cache")
        .withQueryString("action" -> "start")
        .withQueryString("bpdId" -> "25.0ae2047a-5415-41fe-bb45-c3a7908d68e8")
        .withQueryString("branchId" -> "2063.d2b32886-ebb3-42fa-afff-7f4af55c4988")
        .post("{}")
      calls = calls.+:(item)
      Thread.sleep(Math.floor(Math.random * 1500).toLong)
    }
    val pw = new PrintWriter(new FileOutputStream(new File("piid.txt"), true))
    calls.foreach { x =>
      x.map { wsResponse =>
        if (!(200 to 299).contains(wsResponse.status)) {
          sys.error(s"Received unexpected status ${wsResponse.status} : ${wsResponse.body}")
        }
        val piid = (wsResponse.json \ "data" \ "piid").as[String]
        println(s"OK, started process ${piid}")
        pw.println(piid)
        println(s"The response header Content-Length was ${wsResponse.header("Content-Length")}")
      }
    }
    wsClient.close()
  }
}