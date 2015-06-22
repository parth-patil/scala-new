package com.parthpatil

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

object FutureTest extends App {
  def test1(): Unit = {
    // good future1
    val f1 = Future { "foo" }

    // good future2
    val f2 = Future { "bar" }

    // bad future
    val f3 = Future { throw new Exception("Bad!") }

    val fResult: Future[(String, String, String)] = for {
      r1 <- f1
      r2 <- f2
      r3 <- f3
    } yield (r1, r2, r3)

    fResult onComplete {
      case Success(result) =>
        println(result)
      case Failure(e) =>
        println(s"Error : " + e.getMessage)
        println(e.getStackTraceString)
    }
  }

  def test2(): Unit = {

  }

  test1()
}
