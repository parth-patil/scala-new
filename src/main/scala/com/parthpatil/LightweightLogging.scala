package com.parthpatil

import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.JavaConverters._

class LightweightLogger {
  type DeferedString = () => String

  private val buffer = new ConcurrentLinkedQueue[DeferedString]()

  def log(expression: => String): Unit = {
    buffer.add(() => expression)
  }

  def getLinesAndFlush(): Seq[String] = {
    def getBufferContents(): Seq[DeferedString] = buffer.synchronized {
      val output = buffer.iterator.asScala.toSeq
      buffer.clear()
      output
    }

    val contents = getBufferContents()
    contents map { function => function() }
  }
}

object LightweightLogger extends App {
  val logger = new LightweightLogger
  logger.log(s"Expensive compute -> ${100 * 200}")
  logger.log(s"Expensive compute -> ${100.0 / 200}")
  val lines = logger.getLinesAndFlush()
  println(lines mkString ",")
}
