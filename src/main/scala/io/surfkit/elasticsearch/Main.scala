package io.surfkit.elasticsearch

import java.util.UUID

import akka.http.scaladsl.model.HttpRequest
import scala.concurrent.ExecutionContext.Implicits.global
import akka.stream.ActorMaterializer
import akka.actor.{ActorRef, ActorSystem}
import akka.stream.scaladsl._
import play.api.libs.json.Json

import scala.concurrent.Future

/**
  * Created by suroot on
  */
object Main extends App{



  override def main(args: Array[String]) {
    implicit val system = ActorSystem()


    /*val client = GremlinClient.connect()

    client ! GremlinClient.buildRequest("g.V(323600).both().valueMap()")
    client ! GremlinClient.buildRequest("g.V(323600).valueMap()")*/

    println("create client")
    val client = new ESClient(system = system, port = 9400)


    val json =
      """
        |{
        |  "took": 9,
        |  "timed_out": false,
        |  "_shards": {
        |    "total": 5,
        |    "successful": 5,
        |    "failed": 0
        |  },
        |  "hits": {
        |    "total": 15,
        |    "max_score": null,
        |    "hits": [{
        |      "_index": "user",
        |      "_type": "user",
        |      "_id": "2e667bc7-51f7-4733-a609-88b184e0bd71",
        |      "_score": null,
        |      "_source": {
        |        "$type": "m.u.User",
        |        "name": "Rick 2",
        |        "avatar": "https://storage.conversant.im/images/avatars/18.png",
        |        "providers": [{
        |          "$type": "m.u.Email",
        |          "id": "rick2@neverdullventures.com"
        |        }, {
        |          "$type": "m.u.Conversant",
        |          "id": "2e667bc7-51f7-4733-a609-88b184e0bd71"
        |        }],
        |        "crm": [],
        |        "id": "2e667bc7-51f7-4733-a609-88b184e0bd71",
        |        "presence": {
        |          "$type": "m.u.Presence",
        |          "status": "Offline"
        |        }
        |      },
        |      "sort": ["2"],
        |      "inner_hits": {
        |        "identity": {
        |          "hits": {
        |            "total": 1,
        |            "max_score": 5.3488,
        |            "hits": [{
        |              "_index": "user",
        |              "_type": "identity",
        |              "_id": "434d8320-47cf-4986-90ba-9fb0d3564e88.2e667bc7-51f7-4733-a609-88b184e0bd71",
        |              "_score": 5.3488,
        |              "_routing": "2e667bc7-51f7-4733-a609-88b184e0bd71",
        |              "_parent": "2e667bc7-51f7-4733-a609-88b184e0bd71",
        |              "_source": {
        |                "$type": "m.u.Contact",
        |                "team": "434d8320-47cf-4986-90ba-9fb0d3564e88",
        |                "name": "Rick 2",
        |                "providers": [{
        |                  "$type": "m.u.Email",
        |                  "id": "rick2@neverdullventures.com"
        |                }, {
        |                  "$type": "m.u.Conversant",
        |                  "id": "2e667bc7-51f7-4733-a609-88b184e0bd71"
        |                }],
        |                "crm": [{
        |                  "$type": "m.u.CRM",
        |                  "birthday": [],
        |                  "urls": [],
        |                  "company": {},
        |                  "relationships": [],
        |                  "lastName": "",
        |                  "version": "0.1",
        |                  "firstName": "",
        |                  "interests": [],
        |                  "address": {},
        |                  "extra": {},
        |                  "nickname": ""
        |                }]
        |              }
        |            }]
        |          }
        |        }
        |      }
        |    }, {
        |      "_index": "user",
        |      "_type": "user",
        |      "_id": "dfb9af6d-40c3-491f-b323-9bfc3d9138a2",
        |      "_score": null,
        |      "_source": {
        |        "$type": "m.u.User",
        |        "name": "Alex Chuang",
        |        "avatar": "https://conversant.im/assets/images/conversantLogo.png",
        |        "providers": [{
        |          "$type": "m.u.Facebook",
        |          "id": "10101644484027831"
        |        }, {
        |          "$type": "m.u.Conversant",
        |          "id": "dfb9af6d-40c3-491f-b323-9bfc3d9138a2"
        |        }],
        |        "crm": [],
        |        "id": "dfb9af6d-40c3-491f-b323-9bfc3d9138a2",
        |        "presence": {
        |          "$type": "m.u.Presence",
        |          "status": "Offline"
        |        }
        |      },
        |      "sort": ["alex"],
        |      "inner_hits": {
        |        "identity": {
        |          "hits": {
        |            "total": 1,
        |            "max_score": 4.9750576,
        |            "hits": [{
        |              "_index": "user",
        |              "_type": "identity",
        |              "_id": "434d8320-47cf-4986-90ba-9fb0d3564e88.dfb9af6d-40c3-491f-b323-9bfc3d9138a2",
        |              "_score": 4.9750576,
        |              "_routing": "dfb9af6d-40c3-491f-b323-9bfc3d9138a2",
        |              "_parent": "dfb9af6d-40c3-491f-b323-9bfc3d9138a2",
        |              "_source": {
        |                "$type": "m.u.Contact",
        |                "team": "434d8320-47cf-4986-90ba-9fb0d3564e88",
        |                "name": "Alex Chuang",
        |                "providers": [{
        |                  "$type": "m.u.Facebook",
        |                  "id": "10101644484027831"
        |                }, {
        |                  "$type": "m.u.Conversant",
        |                  "id": "dfb9af6d-40c3-491f-b323-9bfc3d9138a2"
        |                }],
        |                "crm": [{
        |                  "$type": "m.u.CRM",
        |                  "birthday": [],
        |                  "urls": [],
        |                  "company": {},
        |                  "relationships": [],
        |                  "lastName": "",
        |                  "version": "0.1",
        |                  "firstName": "",
        |                  "interests": [],
        |                  "address": {},
        |                  "extra": {},
        |                  "nickname": ""
        |                }]
        |              }
        |            }]
        |          }
        |        }
        |      }
        |    }]
        |  }
        |}
      """.stripMargin

    val search = Json.parse(json).as[ES.Search]
    println(s"seasrch: ${search}")

    client.search("user","user", Json.parse(
      """
        |{
        |  "sort": [{
        |    "name": {
        |      "order": "asc"
        |    }
        |  }],
        |  "query": {
        |    "has_child": {
        |      "type": "identity",
        |      "inner_hits": {},
        |      "query": {
        |        "bool": {
        |          "must": [{
        |            "query_string": {
        |              "query": "**"
        |            }
        |          }, {
        |            "query": {
        |              "bool": {
        |                "must": [{
        |                  "bool": {
        |                    "should": [{
        |                      "match": {
        |                        "team": "434d8320-47cf-4986-90ba-9fb0d3564e88"
        |                      }
        |                    }],
        |                    "minimum_number_should_match": 1
        |                  }
        |                }]
        |              }
        |            }
        |          }, {
        |            "query": {
        |              "bool": {
        |                "must": [{
        |                  "bool": {
        |                    "should": [{
        |                      "match": {
        |                        "$type": "m.u.Contact"
        |                      }
        |                    }, {
        |                      "match": {
        |                        "$type": "m.u.Member"
        |                      }
        |                    }],
        |                    "minimum_number_should_match": 1
        |                  }
        |                }]
        |              }
        |            }
        |          }]
        |        }
        |      }
        |    }
        |  }
        |}
      """.stripMargin),Map("size" -> "2", "from" -> "0")).map { r =>
      println("GOT IT !!!!!!!!!")
      println(r)
    }

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
   /* client.search( body = Json.obj(
      "query" -> Json.obj(
        "match" -> Json.obj(
          "content" -> "test"
        )
      )
    )).map{ response =>
      println(response)
    }.recover(ESClient.reportFailure)*/


  /*  val bulk = ES.Bulk.Request(
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
*/
  //  println(ES.Bulk.FormatJson(bulk))

    /*client.bulk("test","raw",bulk).map{ response =>
      println(response)
    }.recover(ESClient.reportFailure)*/
  }

}
