package com.parthpatil

import java.net.ServerSocket
import java.io.PrintWriter
import util.Random

/** Represents a page view on a website with associated dimension data. */
class PageView(url : String, status : Int, country: String, browser: String, userID : Int)
  extends Serializable {
  override def toString() : String = {
    "%s\t%s\t%s\t%s\t%s\n".format(url, status, country, browser, userID)
  }
}

object PageView extends Serializable {
  def fromString(in : String) : PageView = {
    val parts = in.split("\t")
    new PageView(parts(0), parts(1).toInt, parts(2), parts(3), parts(4).toInt)
  }
}

// scalastyle:off
/** Generates streaming events to simulate page views on a website.
  *
  * This should be used in tandem with PageViewStream.scala. Example:
  *
  * To run the generator
  * `$ bin/run-example org.apache.spark.examples.streaming.clickstream.PageViewGenerator 44444 10`
  * To process the generated stream
  * `$ bin/run-example \
  *    org.apache.spark.examples.streaming.clickstream.PageViewStream errorRatePerZipCode localhost 44444`
  *
  */
// scalastyle:on
object PageViewGenerator {
  val pages = Map(
    "http://foo.com/" -> .7,
    "http://foo.com/news" -> 0.2,
    "http://foo.com/contact" -> .1)

  val httpStatus = Map(
    200 -> .95,
    404 -> .05)

  val countries = Map(
    "US" -> .60,
    "CA" -> .05,
    "CH" -> .05,
    "GB" -> .05,
    "IN" -> .05,
    "AU" -> .05,
    "ZB" -> .05,
    "GE" -> .05,
    "MX" -> .05)

  val browsers = Map(
    "FF" -> .20,
    "CH" -> .20,
    "SF" -> .20,
    "OP" -> .20,
    "IE" -> .20)

  val userID = Map((1 to 100).map(_ -> .01) : _*)

  // Choose id=1 & id=2 with higher probability when country is US
  def getRandomUrl(country: String = "") : String = {
    val id = if (country == "US") {
      val range = Random.nextInt(100)
      if (range < 50)
        1
      else if (range < 70)
        2
      else
        2 + Random.nextInt(98)
    } else {
      Random.nextInt(100)
    }
    s"http://www.example.com/id=$id"
  }

  def getRandomUserId(): Int = {
    Random.nextInt(1000000) // 1 mil
  }

  def pickFromDistribution[T](inputMap : Map[T, Double]) : T = {
    val rand = new Random().nextDouble()
    var total = 0.0
    for ((item, prob) <- inputMap) {
      total = total + prob
      if (total > rand) {
        return item
      }
    }
    inputMap.take(1).head._1 // Shouldn't get here if probabilities add up to 1.0
  }

  def getNextClickEvent() : String = {
    val userId = getRandomUserId
    val status = pickFromDistribution(httpStatus)
    val country = pickFromDistribution(countries)
    val url = getRandomUrl(country)
    val browser = pickFromDistribution(browsers)

    /**
    class PageView(url : String, status : Int, country: Int, browser: String, userID : Int)
     */
    new PageView(
      url = url,
      status = status,
      country = country,
      browser = browser,
      userID = userId).toString()
  }

  def main(args : Array[String]) {
    if (args.length != 2) {
      System.err.println("Usage: PageViewGenerator <port> <viewsPerSecond>")
      System.exit(1)
    }
    val port = args(0).toInt
    val viewsPerSecond = args(1).toFloat
    val sleepDelayMs = (1000.0 / viewsPerSecond).toInt
    val listener = new ServerSocket(port)
    println("Listening on port: " + port)

    while (true) {
      val socket = listener.accept()
      new Thread() {
        override def run(): Unit = {
          println("Got client connected from: " + socket.getInetAddress)
          val out = new PrintWriter(socket.getOutputStream(), true)

          while (true) {
            Thread.sleep(sleepDelayMs)
            out.write(getNextClickEvent())
            out.flush()
          }
          socket.close()
        }
      }.start()
    }
  }
}
