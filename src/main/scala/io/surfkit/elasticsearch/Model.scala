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
    def _score: Option[Double]
    def _source: JsValue
  }

  case class Hit(
    _index: String,
    _type: String,
    _id: String,
    _score: Option[Double],
    sort: Option[Seq[JsValue]],
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

  case class IndexLookup(
      _index: String,
      _type: String,
      _id: String,
      _version: Double,
      found: Boolean,
      _source: JsValue
    ) extends IndexBase with ESResponse
  implicit val indexLookupWrites = Json.writes[IndexLookup]
  implicit val indexLookupReads = Json.reads[IndexLookup]


  case class Delete(
        _index: String,
        _type: String,
        _id: String,
        _version: Double,
        found: Boolean,
        _shards: Shards
      ) extends IndexBase with ESResponse
  implicit val DeleteWrites = Json.writes[Delete]
  implicit val DeleteReads = Json.reads[Delete]

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

  object Query{
    sealed trait Val{
      def value: Any
    }
    case class StringVal(value: String) extends Val
    case class IntVal(value: Int) extends Val
    case class DoubleVal(value: String) extends Val
    case class BooleanVal(value: Boolean) extends Val

    sealed trait QueryBase
    sealed trait FieldQuery extends QueryBase{
      def name: String
    }
    case class TermQuery(name: String, value:Val, boost: Option[Double]) extends FieldQuery
    // TODO: index options
    // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-terms-query.html
    case class TermsQuery(name: String, values:Seq[Val], boost: Option[Double]) extends FieldQuery
    case class RangeOp(op: String)
    object RangeOp{
      val gte = RangeOp("gte")
      val gt = RangeOp("gte")
      val lte = RangeOp("gte")
      val lt = RangeOp("gte")
    }
    case class RangeQuery(name: String, rang:Set[RangeOp], value:Val, boost: Option[Double]) extends FieldQuery
    case class ExistsQuery(name: String, value:Val, boost: Option[Double]) extends FieldQuery
    case class PrefixQuery(name: String, value:Val, boost: Option[Double]) extends FieldQuery
    case class WildCardQuery(name: String, value:String, boost: Option[Double]) extends FieldQuery
    case class RegExFlag(flag: String)
    object RegExFlag{
      val ALL = RegExFlag("ALL")
      val ANYSTRING = RegExFlag("ANYSTRING")
      val COMPLEMENT = RegExFlag("COMPLEMENT")
      val EMPTY = RegExFlag("EMPTY")
      val INTERSECTION = RegExFlag("INTERSECTION")
      val INTERVAL = RegExFlag("INTERVAL")
      val NONE = RegExFlag("NONE")
    }
    case class RegExQuery(name: String, value:String, flags:Set[RegExFlag], boost: Option[Double]) extends FieldQuery


    case class Clause(clause: String)
    object Clause{
      val must = Clause("must")
      val should = Clause("should")
      val must_not = Clause("must_not")
    }
    case class Condition(clause:Clause, query:Seq[QueryBase])
    case class BooleanQuery(conditions: Seq[Condition], minimum_should_match: Option[Int], boost:Option[Double]) extends QueryBase

    case class MatchType(name: String)
    object MatchType{
      val default = MatchType("")
      val phrase = MatchType("phrase")
      val phrase_prefix = MatchType("phrase_prefix")
    }

    case class MatchOp(op: String)
    object MatchOp{
      val and = MatchOp("and")
      val or = MatchOp("or")
    }
    case class MatchQuery(field: String, query: String, `type`:MatchType = MatchType.default, analyzer: String, operator: MatchOp = MatchOp.or) extends QueryBase

    case class MultiMatchType(name: String)
    object MultiMatchType{
      val best_fields = MultiMatchType("best_fields")
      val most_fields = MultiMatchType("most_fields")
      val cross_fields = MultiMatchType("cross_fields")
      val phrase = MultiMatchType("phrase")
      val phrase_prefix = MultiMatchType("phrase_prefix")
    }
    case class MultiMatchQuery(fields: Set[String], query: String, `type`:MultiMatchType = MultiMatchType.best_fields, tie_breaker: Option[Double]) extends QueryBase

  }

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
