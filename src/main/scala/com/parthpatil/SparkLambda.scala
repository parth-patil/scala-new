package com.parthpatil

import org.json4s._
import org.json4s.jackson._
import org.json4s.jackson.Serialization._

import scala.util.Random

class SparkLambda {
}

/**
{
  "created": 1433262971414,
  "url": "http://www.example.com/id=1",
  "country": "US",
  "browser": "FF"
}
*/
case class LogLine(created: Long, url: String, country: String, browser: String)

object SparkLambda extends App {
  implicit val formats = DefaultFormats

  def genFakeData(numRows: Int): Unit = {
    val countries = Array("CA", "CH", "GB", "IN", "AU", "ZB", "GE", "MX")
    val countriesSize = countries.size

    val browsers = Array("FF", "CH", "SF", "OP", "IE")
    val browsersSize = browsers.size

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

    // Choose "US" with 50% probability
    def getRandomCountry: String = {
      if (Random.nextBoolean())
        "US"
      else {
        val idx = Random.nextInt(countriesSize)
        countries(idx)
      }
    }

    def getRandomBrowser: String = {
      val idx = Random.nextInt(browsersSize)
      browsers(idx)
    }

    var currentTime: Long = 0

    for (i <- 0 to numRows) {
      val country = getRandomCountry
      val url = getRandomUrl(country)
      val browser = getRandomBrowser

      val row = Map(
        "created" -> s"$currentTime",
        "url" -> url,
        "country" -> country,
        "browser" -> browser
      )

      println(write(row))

      currentTime += 1
    }
  }

  genFakeData(100)
}
