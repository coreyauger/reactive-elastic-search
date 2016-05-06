package io.surfkit.elasticsearch

import java.io.IOException
import java.net.URLEncoder
import java.util.UUID

import akka.Done
import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding
import akka.http.scaladsl.model.HttpHeader.ParsingResult
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.ws._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.{QueueOfferResult, IOResult, ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl._
import play.api.libs.json.{JsValue, Reads, Json}
import scala.concurrent.{Future, Await, Promise}
import scala.util.{Failure, Success}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration

/**
  * Created by coreyauger
  */

// TODO: take a look at pipelining-limit = 1

object ESClient{
  /*def buildRequest(gremlin: String, bindings: Map[String,String] = Map.empty[String, String] ) = {
    Gremlin.Request(
      requestId = UUID.randomUUID,
      op = Gremlin.Op.eval,
      processor = "",
      args = Gremlin.Args(
        gremlin = gremlin,
        bindings = bindings,
        language = Gremlin.Language.`gremlin-groovy`
      )
    )
  }*/

  val reportFailure:scala.PartialFunction[scala.Throwable, Unit] = {
    case t:Throwable =>
      println("[ERROR]: " + t.getMessage)
      t.printStackTrace()
  }
}

class ESClient(host:String = "localhost", port: Int = 9200, responder:Option[ActorRef] = None, implicit val system: ActorSystem = ActorSystem()) {
  import ES._


  implicit val materializer = ActorMaterializer()

  private[this] val poolClientFlow = Http().cachedHostConnectionPool[Promise[HttpResponse]](host = host, port = port)
  private[this] val queue = Source.queue[(HttpRequest, Promise[HttpResponse])](10, OverflowStrategy.dropNew)
    .via(poolClientFlow)
    .toMat(Sink.foreach({
      case ((Success(resp), p)) => p.success(resp)
      case ((Failure(e), p)) => p.failure(e)
    }))(Keep.left)
    .run

  def shutdown = {
    Http().shutdownAllConnectionPools()
  }

  def request(req: HttpRequest = HttpRequest(uri = "/")):Future[String] = {
    println(s"requesting: ${req.uri}")
    val promise = Promise[HttpResponse]
    val request = req -> promise

    val response = queue.offer(request).flatMap(buffered => {
      if (buffered == QueueOfferResult.Enqueued) promise.future
      else Future.failed(new RuntimeException())
    })
    response.flatMap{ r =>
      r.status match {
        case OK => Unmarshal(r.entity).to[String]
        case Created => Unmarshal(r.entity).to[String]
        case _ => Unmarshal(r.entity).to[String].flatMap { entity =>
          val error = s"[ERROR] - Core HTTP request failed with status code (${r.status}) and entity '$entity'"
          println(error)
          Future.failed(new IOException(error))
        }
      }
    }
  }

  def header(key: String, value: String): Option[HttpHeader] =
    HttpHeader.parse(key, value) match {
      case ParsingResult.Ok(header, errors) ⇒ Option(header)
      case _                                ⇒ None
    }

  def headers(headersMap: Map[String, String]): List[HttpHeader] =
    headersMap.flatMap {
      case (key, value) ⇒ header(key, value)
    }.toList

  def mkEntity(body: String): HttpEntity.Strict = HttpEntity(ContentTypes.`application/json`, body)

  def mkRequest(requestBuilder: RequestBuilding#RequestBuilder, url: String, body: String = "", queryParamsMap: Map[String, String] = Map.empty, headersMap: Map[String, String] = Map.empty) =
    requestBuilder(url + queryString(queryParamsMap), mkEntity(body))

  def queryString(p:Map[String, String]) =
    p.map(x => s"${x._1}=${URLEncoder.encode(x._2,"UTF-8")}").mkString("?","&","")

  def api[T <: ES.ESResponse](req: HttpRequest)(implicit fjs: Reads[T]):Future[T] = request(req).map(s => fjs.reads(Json.parse(s)).get )

  def health(params: Map[String, String] = Map.empty[String, String]):Future[ES.Health] =
    api[ES.Health](HttpRequest(uri = s"/_cluster/health${queryString(params)}"))

  def search(index: String = "", `type`: String = "", body: JsValue, params: Map[String, String] = Map.empty[String, String]):Future[ES.Search] = {
    val uri = List(index, `type`, "_search").mkString("/","/","") + queryString(params)
    println(body.toString)
    val req = mkRequest(RequestBuilding.Post, uri, body.toString, params)
    api[ES.Search](req)
  }

  def searchLite(index: String = "", `type`: String = "", query: String, params: Map[String, String] = Map.empty[String, String]):Future[ES.Search] = {
    val uri = List(index, `type`, "_search").mkString("/","/","") + queryString(params + ("q" -> query))
    val req = mkRequest(RequestBuilding.Get, uri, "", params)
    api[ES.Search](req)
  }

  // TODO: what about custom mappings...
  // https://www.elastic.co/guide/en/elasticsearch/guide/current/mapping-intro.html
  def indexJs(index: String, `type`: String, id: String, json: JsValue, params: Map[String, String] = Map.empty[String, String]):Future[ES.IndexCreate] =
    this.index(index, `type`, id, json.toString, params)

  def index(index: ES.Index, json: JsValue, params: Map[String, String]):Future[ES.IndexCreate] =
    this.index(index._index, index._type, index._id, json.toString, params)

  def index(index: String, `type`: String, id: String, json: String, params: Map[String, String] = Map.empty[String, String]):Future[ES.IndexCreate] = {
    val uri = List(index, `type`, id).mkString("/","/","")
    val req = mkRequest(RequestBuilding.Put, uri, json, params)
    api[ES.IndexCreate](req)
  }


  def delete(index: ES.Index, params: Map[String, String]):Future[ES.Ack] =
    this.delete(index._index, index._type, index._id, params)

  def delete(index: String, `type`: String = "", id: String = "", params: Map[String, String] = Map.empty[String, String]):Future[ES.Ack] = {
    val uri = List(index, `type`, id).mkString("/","/","")
    val req = mkRequest(RequestBuilding.Delete, uri, "", params)
    api[ES.Ack](req)
  }


  def analyze(analyzer: String = "standard", text: String, params: Map[String, String] = Map.empty[String, String]): Future[ES.Tokens] =
    api[ES.Tokens](HttpRequest(uri = s"/_analyze${queryString(params ++ Map("analyzer" -> analyzer, "text" -> text))}"))

  def mapping(index: String, `type`: String, params: Map[String, String] = Map.empty[String, String]):Future[JsValue] = {
    val uri = List(index, "_mapping", `type`).mkString("/","/","") + queryString(params)
    request(HttpRequest(uri = uri)).map(x => Json.parse(x))
  }

  def bulk(index: String = "", `type`: String, bulk: ES.Bulk.Request, params: Map[String, String] = Map.empty[String, String]):Future[JsValue] = {
    val uri = List(index, `type`, "_bulk").mkString("/","/","") + queryString(params)
    val req = mkRequest(RequestBuilding.Post, uri, ES.Bulk.FormatJson(bulk), params)
    request(req).map(x => Json.parse(x))
  }

}
