package io.surfkit.elasticsearch

import java.util.UUID

import akka.http.scaladsl.model.HttpRequest
import scala.concurrent.ExecutionContext.Implicits.global
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import play.api.libs.json.Json

import scala.concurrent.Future

/**
  * Created by suroot on
  */
object Main extends App{



  override def main(args: Array[String]) {

    /*val client = GremlinClient.connect()

    client ! GremlinClient.buildRequest("g.V(323600).both().valueMap()")
    client ! GremlinClient.buildRequest("g.V(323600).valueMap()")*/

    println("create client")
    val client = new ESClient()

    /*client.indexJs("test","raw", UUID.randomUUID().toString, Json.obj(
      "author" -> "Corey Auger",
      "date" -> "Aug 2 2015",
      "text" -> "This is some basic text that we are going to use to test."
    )).map{response =>
      println(response)
    }.recover(ESClient.reportFailure)


    client.delete("test","raw").map{response =>
      println(response)
    }.recover(ESClient.reportFailure)*/
    /*client.analyze(text = "This is the biggest bad test around.").map{ r =>
      println(r)
    }.recover(ESClient.reportFailure)

    client.mapping(index = "test", `type` = "raw").map{ r =>
      println(r)
    }.recover(ESClient.reportFailure)

    println("request health")
    client.health().foreach{ response =>
      println(response)
    }
    client.request(HttpRequest(uri = "/_cluster/health?pretty&wait_for_status=green&timeout=5s")).foreach{ response =>
      println(response)
    }*/
    client.search( body = Json.obj(
      "query" -> Json.obj(
        "match" -> Json.obj(
          "content" -> "test"
        )
      )
    )).map{ response =>
      println(response)
    }.recover(ESClient.reportFailure)



    val bulk = ES.Bulk.Request(
      Seq(
        ES.Bulk.Entry(ES.Bulk.Action(ES.Bulk.ActionType.index, ES.Index("test","raw",UUID.randomUUID().toString)), Some(
          Json.obj(
            "author" -> "Corey Auger",
            "date" -> "xyz",
            "text" -> "Here is a random insert that I am testing."
          )
        )),
        ES.Bulk.Entry(ES.Bulk.Action(ES.Bulk.ActionType.index, ES.Index("test","raw",UUID.randomUUID().toString)), Some(
          Json.obj(
            "author" -> "John Doe",
            "date" -> "xyz",
            "text" -> "222222"
          )
        ))
      )
    )

    println(ES.Bulk.FormatJson(bulk))

    /*client.bulk("test","raw",bulk).map{ response =>
      println(response)
    }.recover(ESClient.reportFailure)*/
  }

}
