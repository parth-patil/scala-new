package com.parthpatil

import com.lambdaworks.redis.output.{KeyStreamingChannel, KeyValueStreamingChannel}
import com.lambdaworks.redis.pubsub.{RedisPubSubConnection, RedisPubSubAdapter, RedisPubSubListener}
import com.lambdaworks.redis.{ScriptOutputType, MapScanCursor, RedisFuture, RedisClient}
import rx.lang.scala.Observable
import scala.concurrent.{Promise, Future, Await, ExecutionContext}
import scala.concurrent.duration._
//import scala.concurrent.ExecutionContext.Implicits.global
import com.google.common.util.concurrent._

import java.util.concurrent.{ExecutorService, Executors}

import scala.util.{Failure, Success}
import java.util.{ArrayList => JArraylist}

object RxRedis extends App {
  implicit def guavaFutureToScalaFuture[T](gFuture: ListenableFuture[T])
                                          (implicit executor: ListeningExecutorService): Future[T] = {
    val p = Promise[T]()
    Futures.addCallback[T](gFuture, new FutureCallback[T] {
      def onSuccess(s: T)         { p.success(s) }
      def onFailure(e: Throwable) { p.failure(e) }
    }, executor)
    p.future
  }

  val executorService: ExecutorService = Executors.newFixedThreadPool(4)
  implicit val executionContext = ExecutionContext.fromExecutorService(executorService)
  implicit val executor = MoreExecutors.listeningDecorator(executorService)
  val client = new RedisClient("127.0.0.1")
  val asyncConnection = client.connectAsync()

  def shutdown(): Unit = {
    asyncConnection.close()
    client.shutdown()
    executorService.shutdown()
  }

  def testSimpleGetSet(): Unit = {
    asyncConnection.get("k1").onComplete {
      case Success(r) => println(s"k1 -> $r")
      case Failure(e) => println(e)
    }
  }

  def testPubSub(): Unit = {
    val testChannel = "chan1"

    Observable.interval(1 second) subscribe { x =>
      asyncConnection.publish(testChannel, System.currentTimeMillis.toString) onSuccess { case r =>
        println(s"Num clients received = $r")
      }
    }

    val obs = Observable[(String, String)] { subscriber =>
      val connection: RedisPubSubConnection[String, String] = client.connectPubSub()
      connection.addListener(new RedisPubSubAdapter[String, String]() {
        override def message(chan: String, msg: String): Unit = {
          if (!subscriber.isUnsubscribed)
            subscriber.onNext((chan, msg))
        }
      })
      connection.subscribe(testChannel)
    }

    obs subscribe { x => println(x) }
  }

  def testHscan(): Unit = {
    val hashKey = "hash1"
    Observable[(String, String)] { subscriber =>
      asyncConnection.hscan(new KeyValueStreamingChannel[String, String]() {
        override def onKeyValue(key: String, value: String) {
          if (!subscriber.isUnsubscribed)
            subscriber.onNext((key, value))
        }
      }, hashKey)
    } subscribe { kv =>
      println(s"key => ${kv._1}, value = ${kv._2}")
    }
  }

  def testScanKeys(): Unit = {
    Observable[String] { subscriber =>
      asyncConnection.scan(new KeyStreamingChannel[String]() {
        override def onKey(key: String) {
          if (!subscriber.isUnsubscribed)
            subscriber.onNext(key)
        }
      })
    } subscribe { key => println(s"key => $key") }
  }

  def testEval(): Unit = {
    val result = Await.result(asyncConnection.eval[Long]("return 1 + 1", ScriptOutputType.INTEGER), 2 second)
    println(s"result = $result")

    val script = """return redis.call('zrevrange', KEYS[1], 0, 1)"""
    val script2 =
      """
        |if false then
        | return 'foo'
        |else
        | return nil
        |end
      """.stripMargin

    val script3 =
      """
        |local zset_key = KEYS[1]
        |local reverse_current_ts = ARGV[1]
        |local max_ts = ARGV[2]
        |
        |local top_item_wrapped = redis.call('ZREVRANGE', zset_key, 0, 0)
        |local table_size = table.maxn(top_item_wrapped)
        |
        |if (table_size > 0 and top_item_wrapped[1] > reverse_current_ts) then
        |  local result = redis.call('ZRANGEBYSCORE', zset_key, reverse_current_ts, max_ts)
        |  redis.call('ZREMRANGEBYSCORE', zset_key, reverse_current_ts, max_ts)
        |  return result
        |end
      """.stripMargin

    val maxTs = 1000
    val reverseCurrentTs = maxTs - 997

    val result2 = Await.result(
      asyncConnection.eval[JArraylist[String]](
        script3, ScriptOutputType.MULTI, Array("sorted1"), reverseCurrentTs.toString, maxTs.toString), 2 second)

    println(s"result2 = ${result2}")
    shutdown()
  }

  //testPubSub()
  //testHscan()
  //testScanKeys()
  testEval()
}

/**
implicit class ObservableOps[T](obs: Observable[T]) {
    def timedOut(d: Duration): Observable[T] = {
      val stopper = Observable.interval(d)
      obs.takeUntil(stopper)

    }

    def completeAfterInactivity(d: Duration): Observable[T] = {
      def currentTs = System.currentTimeMillis()
      val checker = Observable.interval(100 millisecond)
      var lastUpdated = currentTs
      obs map { item =>
        lastUpdated = currentTs
        item
      }
    }
  }
 */
