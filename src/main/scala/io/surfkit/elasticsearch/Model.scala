package io.surfkit.elasticsearch

import java.util.UUID
import play.api.libs.json._

/**
  * Created by coreyauger
  */
object ES {

  sealed trait ESResponse

  case class Health(
    cluster_name: String,
    status: String,
    timed_out: Boolean,
    number_of_nodes: Int,
    number_of_data_nodes: Int,
    active_primary_shards: Int,
    active_shards: Int,
    relocating_shards: Int,
    initializing_shards: Int,
    unassigned_shards: Int,
    number_of_pending_tasks: Int,
    number_of_in_flight_fetch: Int
  ) extends ESResponse
  implicit val healthWrites = Json.writes[Health]
  implicit val healthReads = Json.reads[Health]

  case class Ack(acknowledged:Boolean) extends ESResponse
  implicit val ackWrites = Json.writes[Ack]
  implicit val ackReads = Json.reads[Ack]

  trait IndexBase extends ESResponse{
    def _index: String
    def _type: String
    def _id: String
  }

  trait HitBase extends IndexBase{
    def _score: Double
    def _source: JsValue
  }

  case class Hit(
    _index: String,
    _type: String,
    _id: String,
    _score: Double,
    _source: JsValue) extends HitBase
  implicit val hitWrites = Json.writes[Hit]
  implicit val hitReads = Json.reads[Hit]

  case class Hits(
    total: Int,
    hits: Seq[Hit],
    max_score: Option[Double]) extends ESResponse
  implicit val hitsWrites = Json.writes[Hits]
  implicit val hitsReads = Json.reads[Hits]

  case class Shards(
    failed: Int,
    successful: Int,
    total: Int) extends ESResponse
  implicit val shardsWrites = Json.writes[Shards]
  implicit val shardsReads = Json.reads[Shards]

  case class Search(
    hits: Hits,
    took: Int,
    _shards: Shards,
    timed_out: Boolean) extends ESResponse
  implicit val searchResponseWrites = Json.writes[Search]
  implicit val searchResponseReads = Json.reads[Search]

  case class IndexCreate(
    _index: String,
    _type: String,
    _id: String,
    _version: Double,
    created: Boolean) extends IndexBase
  implicit val indexCreateWrites = Json.writes[IndexCreate]
  implicit val indexCreateReads = Json.reads[IndexCreate]

  case class Index(
    _index: String,
    _type: String,
    _id: String) extends IndexBase
  implicit val indexWrites = Json.writes[Index]
  implicit val indexReads = Json.reads[Index]

  case class Token(
    token: String,
    start_offset: Int,
    end_offset: Int,
    `type`: String,
    position: Int) extends ESResponse
  implicit val tokenWrites = Json.writes[Token]
  implicit val tokenReads = Json.reads[Token]

  case class Tokens(tokens: Seq[Token]) extends ESResponse
  implicit val tokensWrites = Json.writes[Tokens]
  implicit val tokensReads = Json.reads[Tokens]

  object Bulk{
    import play.api.libs.json._
    import play.api.libs.functional.syntax._

    case class ActionType(action: String)
    object ActionType{
      val index = ActionType("index")
      val create = ActionType("create")
      val update = ActionType("update")
      val delete = ActionType("delete")
    }
    case class Action(action: ActionType, index: Index)
    case class Entry(action: Action, source: Option[JsValue])
    case class Request(entries: Seq[Entry])

    def FormatJson(request: Request): String = {
      request.entries.map{ entry =>
        val ind = Json.toJson(entry.action.index)
        s"""{"${entry.action.action.action}": ${ind}}""" +
        entry.source.map{ src =>
          s"""\n${src}"""
        }.getOrElse("")
      }.mkString("\n") + "\n"
    }
  }


  /*case class Op(op: String)
  object Op{
    val eval = Op("eval")
  }
  implicit val opWrites = new Writes[Op] {
    def writes(op: Op) = JsString(op.op)
  }
  implicit val opReads:Reads[Op] = (JsPath \ "op").read[String].asInstanceOf[Reads[Op]]*/


}
