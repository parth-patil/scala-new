package com.parthpatil

import java.io.File
import java.util.concurrent.LinkedBlockingQueue

import org.apache.commons.io.input._
import rx.lang.scala.Observable

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._
import scala.util._

class MyTailer(filePath: String, handler: String => Unit, pollInterval: Duration, tailFromEnd: Boolean = true) {
  val listener = new TailerListenerAdapter {
    override def handle(line: String) { handler(line) }
  }

  val tailer = Tailer.create(new File(filePath), listener, pollInterval.toMillis, tailFromEnd)

  def stop(): Unit = { tailer.stop() }
}

class FutureTailer(filePath: String, pollInterval: Duration) {
  // Create a queue to collect the tailed lines
  val queue = new LinkedBlockingQueue[String]

  val tailer = new MyTailer(filePath, { queue.add(_) }, pollInterval)

  def next(): Future[String] = Future { blocking { queue.take() } }
}

object LogTailer extends App {
  def createFutureTailer(filePath: String, pollInterval: Duration): FutureTailer = {
    new FutureTailer(filePath, pollInterval)
  }

  def createObservableTailer(filePath: String, pollInterval: Duration): Observable[String] = {
    Observable { subscriber =>
      def handler(line: String): Unit = {
        if (!subscriber.isUnsubscribed)
          subscriber.onNext(line)
      }
      new MyTailer(filePath, handler, pollInterval)
    }
  }


  def testFutureTailer(file: String) {
    // Usage of FutureTailer
    val fTailer = createFutureTailer(file, 100 millisecond)

    def onCompleteHandler(tryLine: Try[String]): Unit = {
      tryLine match {
        case Success(line) =>
          fTailer.next() onComplete onCompleteHandler
          println(s"---------- $line ------------")

        case Failure(e) =>
      }
    }

    fTailer.next() onComplete onCompleteHandler
  }

  def testObservableTailer(file: String): Unit = {
    val oTailer = createObservableTailer(file, 100 millisecond)
    oTailer foreach { line =>
      println(s"---------- $line ------------")
    }
  }

  val file = "/tmp/growing_log_file.log"
  //testFutureTailer(file)
  testObservableTailer(file)

  // Following is for the program from exiting
  Thread.sleep(1000 * 30)
}
