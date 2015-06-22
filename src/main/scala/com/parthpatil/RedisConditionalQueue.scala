package com.parthpatil

import com.lambdaworks.redis._
import rx.lang.scala.Observable
import scala.concurrent.{Promise, Future, Await, ExecutionContext}
import scala.concurrent.duration._

import com.google.common.util.concurrent._

import java.util.concurrent.{TimeUnit, ExecutorService, Executors}

import scala.util.{Failure, Random, Try, Success}
import java.util.{ArrayList => JArraylist}
import scala.collection.JavaConversions._

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

class RedisConditionalQueue(
  asyncConnection: RedisAsyncConnection[String, String],
  conditionCheckingLuaScript: Option[String],
  zsetKey: String,
  executorService: ExecutorService) {

  import RedisConditionalQueue._

  implicit val executionContext = ExecutionContext.fromExecutorService(executorService)
  implicit val executor = MoreExecutors.listeningDecorator(executorService)

  // Wait for the Lua script to be loaded into the server's script cache
  val script = conditionCheckingLuaScript getOrElse defaultConditionalScript
  val sha1 = Await.result(asyncConnection.scriptLoad(script), 2 second)

  /**
   * Default Lua 5.1 script that is used for checking the condition
   * to pop items off the queue
   * @return
   */
  def defaultConditionalScript(): String = {
    """
      |local zset_key = KEYS[1]
      |local reverse_current_ts = ARGV[1]
      |local max_ts = ARGV[2]
      |
      |-- Get all items older than current timestamp
      |local arr = redis.call('ZRANGEBYSCORE', zset_key, reverse_current_ts, max_ts)
      |local arr_size = table.maxn(arr)
      |
      |if (arr_size > 0) then
      |  -- Delete these items from the zset
      |  redis.call('ZREMRANGEBYSCORE', zset_key, reverse_current_ts, max_ts)
      |  return arr
      |else
      | return {}
      |end
    """.stripMargin
  }

  /**
   * Retuns an Observable interface to the redis conditional Zset
   *
   * @param pollInterval
   * @return
   */
  def getObservableQueue(pollInterval: Duration): Observable[Task] = {
    /*
    for {
      _ <- Observable.interval(pollInterval)
      tasks <- Observable.from(getTasks) // Get Observable from Future[Seq[Task]]
      flattened <- Observable.from(tasks) // Get Observable from Seq[Task]
    } yield {
      flattened
    }
    */

    Observable.interval(pollInterval) flatMap { i =>
      Observable[Task] { subscriber =>
        getTasks foreach { tasks =>
          tasks foreach { subscriber.onNext(_) }
        }
      }
    }
  }

  /**
   * Returns a list of items ready to be processed or an empty list
   * @return
   */
  def getTasks(): Future[Seq[Task]] = {
    implicit val formats = DefaultFormats

    val result: Future[JArraylist[String]] =
      asyncConnection.evalsha[JArraylist[String]](
        sha1,
        ScriptOutputType.MULTI,
        Array(zsetKey),
        reverseCurrentTs.toString,
        MAX_EPOCH_TIME.toString)

    result map { aList =>
      aList.toIndexedSeq flatMap { jsonString =>
        //println(s"**** jsonString = $jsonString")
        Try { parse(jsonString).extract[Task] } toOption
      }
    }
  }

  /**
   * When the processing of an item fails the client is responsible for
   * calling enqueue to schedule processing of the item in the future
   * @param task
   * @param nextAttemptTs timestamp of when to attempt the processing of this job in the future
   * @return
   */
  def enqueue(task: Task, nextAttemptTs: Long): Future[java.lang.Long] = {
    //implicit val formats = DefaultFormats

    val reverseNextAttemptTs = MAX_EPOCH_TIME - nextAttemptTs
    val serialized: String = compact(render(task.toJValue))
    //println(s"enqueue: serialized = $serialized, reverserN = $reverseNextAttemptTs")

    // @todo put the num attempts in redis and use a lua script to re enqueue an item
    asyncConnection.zadd(zsetKey, reverseNextAttemptTs, serialized)
  }

  def reverseCurrentTs(): Long = MAX_EPOCH_TIME - System.currentTimeMillis
}

object RedisConditionalQueue extends App {
  val MAX_EPOCH_TIME = Int.MaxValue * 1000L
  val MAX_ALLOWED_FAILURES = 2

  case class Task(created: Long, numFailures: Int, payload: String) {
    def toJValue(): JValue = {
      ("created" -> created) ~
      ("numFailures" -> numFailures) ~
      ("payload" -> payload)
    }

    override def toString(): String = {
      compact(render(toJValue))
    }
  }

  implicit def guavaFutureToScalaFuture[T](gFuture: ListenableFuture[T])
                                          (implicit executor: ListeningExecutorService): Future[T] = {
    val p = Promise[T]()
    Futures.addCallback[T](gFuture, new FutureCallback[T] {
      def onSuccess(s: T)         { p.success(s) }
      def onFailure(e: Throwable) { p.failure(e) }
    }, executor)
    p.future
  }

  /**
   * Method to populate sample Tasks in the job queue
   * @param rcq
   */
  def populateItems(rcq: RedisConditionalQueue): Unit = {
    (0 to 10) map { i =>
      val payload = s"item-$i"
      val task = Task(created = System.currentTimeMillis(), numFailures = 0, payload = payload)
      val nextAttemptTs = System.currentTimeMillis + i * 1000
      //println(s"Inserting task = $task, nextAttemptTs = $nextAttemptTs")
      Await.result(
        rcq.enqueue(task, nextAttemptTs),
        1 second)
    }
  }

  /**
   * Dummy Task processor
   * @param t
   * @return
   */
  def processTask(t: Task)(implicit ec: ExecutionContext): Future[Boolean] = {
    Future {
      if (Random.nextBoolean())
        true
      else
        throw new Exception("Task Failed")
    }
  }

  def run(): Unit = {
    val executorService: ExecutorService = Executors.newFixedThreadPool(4)
    implicit val executionContext = ExecutionContext.fromExecutorService(executorService)
    val client = new RedisClient("127.0.0.1")
    val asyncConnection = client.connectAsync()

    val rcq = new RedisConditionalQueue(
      asyncConnection = asyncConnection,
      conditionCheckingLuaScript = None,
      zsetKey = "sorted1",
      executorService = executorService)

    populateItems(rcq)
    rcq.getObservableQueue(pollInterval = 1 second) subscribe { task =>
      // Process the task
      println(s"received task -> $task ")

      // If the task fails enqueue it back with a timestamp in the future
      processTask(task) onComplete {
        case Success(_) =>
          println(s"Job Success!, task = $task")
        case Failure(e) =>
          println(s"Job Failed!, task = $task")
          val totalFailures = task.numFailures + 1
          val newTask = task.copy(numFailures = totalFailures)
          if (totalFailures < MAX_ALLOWED_FAILURES) {
            val nextAttemptTs = System.currentTimeMillis + 2 * totalFailures * 1000
            println(s"Reenqueued -> $newTask")
            rcq.enqueue(newTask, nextAttemptTs)
          } else {
            println(s"Discarding task -> $newTask, totalFailures = $totalFailures")
          }
      }
    }
  }

  run()
}
