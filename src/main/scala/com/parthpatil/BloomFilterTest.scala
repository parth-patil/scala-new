package com.parthpatil

import java.util.zip.GZIPOutputStream

import org.apache.commons.codec.binary.Base64
import java.security.MessageDigest
import com.google.common.hash.{Funnels, BloomFilter}
import java.io._

class MyBloomFilter(bf: BloomFilter[Array[Byte]]) {
  def this(numItems: Int, errorPercent: Double) = {
    this(BloomFilter.create(Funnels.byteArrayFunnel, numItems, errorPercent))
  }

  private var cardinality = 0
  val md5 = MessageDigest.getInstance("MD5")

  def genHash(str: String): Array[Byte] = md5.digest(str.getBytes)

  def size = cardinality

  def put(item: String): Unit = {
    bf.put(genHash(item))
    cardinality += 1
  }

  def mightContain(item: String): Boolean = bf.mightContain(genHash(item))

  def serializeToBase64(): String = {
    val bs = new ByteArrayOutputStream
    bf.writeTo(bs)
    val bfByteArray = bs.toByteArray
    bs.close()
    Base64.encodeBase64String(bfByteArray)
  }
}

object MyBloomFilter {
  def fromBase64(serialized: String): MyBloomFilter = {
    val bfByteArray = Base64.decodeBase64(serialized)
    val inputStream = new ByteArrayInputStream(bfByteArray)
    val bf = BloomFilter.readFrom(inputStream, Funnels.byteArrayFunnel)
    new MyBloomFilter(bf)
  }
}

class BloomFilterTest {
  def compressByteArray(arr: Array[Byte]): Array[Byte] = {
    val byteStream = new ByteArrayOutputStream(arr.length)

    try
    {
      val zipStream = new GZIPOutputStream(byteStream)
      try
      {
        zipStream.write(arr)
      }
      finally
      {
        zipStream.close()
      }
    }
    finally
    {
      byteStream.close()
    }

    byteStream.toByteArray()
  }

  def serializeBf[T](bf: BloomFilter[T]): Array[Byte] = {
    val bs = new ByteArrayOutputStream
    bf.writeTo(bs)
    bs.toByteArray
  }

  def test1() {
    import java.security.MessageDigest
    val digest = MessageDigest.getInstance("MD5").digest("foo".getBytes)
    val encodedBytes = Base64.encodeBase64(digest)
    val encodedString = new String(encodedBytes)

    val bf = BloomFilter.create(Funnels.byteArrayFunnel, 1000, 0.01)

    // Put 25 items in the bloom filter
    for (i <- 1 to 1000) {
      val digest = MessageDigest.getInstance("MD5").digest(s"$i".getBytes)
      bf.put(digest)
      val bs = new ByteArrayOutputStream
      bf.writeTo(bs)
      val bfByteArray = bs.toByteArray
      val compressed = compressByteArray(bfByteArray)
      val size = bfByteArray.size
      val compressedSize = compressed.size

      val encodedBytes = Base64.encodeBase64(bfByteArray)
      val encodedString = new String(encodedBytes)

      println(s"i = $i, size = $size, compressed size = $compressedSize, base64BF length = " + encodedString.size)
    }

    // Check phase
    var numFound = 0
    for (i <- 1 to 1000) {
      val digest = MessageDigest.getInstance("MD5").digest(s"$i".getBytes)
      if (bf.mightContain(digest))
        numFound += 1
    }
    println(s"numFound = $numFound")
    /*
    bf.put(digest)

    println(s"contains(foo) = " + bf.mightContain(digest))

    val digest2 = MessageDigest.getInstance("MD5").digest("bar".getBytes)
    bf.mightContain(digest2)
    println(s"contains(bar) = " + bf.mightContain(digest2))

    val bs = new ByteArrayOutputStream

    val foo = bf.writeTo(bs)
    val bytes = bs.toByteArray
    println(s"num bytes after serialization = " + bytes.size)
    */
  }

  def binMd5(str: String): Array[Byte] = {
    MessageDigest.getInstance("MD5").digest(str.getBytes)
  }

  def getId(i: Int) = s"id-$i"

  def testBfMerge(): Unit = {
    def createBf(items: Seq[String]): BloomFilter[Array[Byte]] = {
      val bf = BloomFilter.create(Funnels.byteArrayFunnel, 100, 0.01)
      items foreach { item => bf.put(binMd5(item)) }
      bf
    }

    val bf = BloomFilter.create(Funnels.byteArrayFunnel, 100, 0.01)
    (1 to 100).sliding(10, 10) foreach { items =>
      val strItems = items map getId
      val newBf = createBf(strItems)
      bf.putAll(newBf)
    }

    // Check phase
    var numFound = 0
    (1 to 1000) foreach { i =>
      val id = getId(i)
      val md5 = binMd5(id)
      if (bf.mightContain(md5))
        numFound += 1
    }

    println(s"Num found = $numFound")
  }

  def testNumItemsVsCompression(): Unit = {
    val bf = BloomFilter.create(Funnels.byteArrayFunnel, 1000, 0.01)
    for (i <- 1 to 1000) {
      val id = getId(i)
      bf.put(id.getBytes)
      val serialized = serializeBf(bf)
      val compressed = compressByteArray(serialized)
      println(s"i = $i, compressed = " + compressed.size)
    }
  }

  def testUsingCompressedBitSet(): Unit = {

  }

  def testErrorRateWithManyFilters(): Unit = {
    val filters: Seq[BloomFilter[Array[Byte]]] = (0 until 10) map { i =>
      BloomFilter.create(Funnels.byteArrayFunnel, 10, 0.01)
    }

    for (i <- 0 until 100) {
      val index = (i / 10).toInt
      println(s"inserting $i into filter $index")
      filters(index).put(s"$i".getBytes)
    }

    var numFound = 0
    for (i <- 0 until 1000; j <- 0 until 10) {
      if (filters(j).mightContain(s"$i".getBytes))
        numFound += 1
    }
    println(s"numFound = $numFound, % error = " + (numFound - 100)/ 1000.0)
  }

  def testErrorFor100ItemFilter(): Unit = {
    val filter = BloomFilter.create(Funnels.byteArrayFunnel, 100, 0.01)

    for (i <- 0 until 100) {
      filter.put(s"$i".getBytes)
    }

    var numFound = 0
    for (i <- 0 until 2000) {
      if (filter.mightContain(s"$i".getBytes))
        numFound += 1
    }

    println(s"numFound = $numFound, % error = " + (numFound - 100)/2000.0)
  }
}

//val bft = new BloomFilterTest
//bft.test1
//bft.testBfMerge()
//bft.testNumItemsVsCompression()
//bft.testErrorRateWithManyFilters()
//bft.testErrorFor100ItemFilter()

object BloomFilterTest extends App {
  val myBloom = new MyBloomFilter(100, 0.01)
  myBloom.put("http://www.google.com")
  myBloom.put("http://www.yahoo.com")

  val encodedStr = myBloom.serializeToBase64()
  println(s"size = " + encodedStr.size)

  // Recreate the Bloom Filter from the Base64 encoded string
  val myBloom2 = MyBloomFilter.fromBase64(encodedStr)

  println(s"google exits -> " + myBloom2.mightContain("http://www.google.com"))
  println(s"yahoo exits -> " + myBloom2.mightContain("http://www.yahoo.com"))
  println(s"microsoft exits -> " + myBloom2.mightContain("http://www.microsoft.com"))
}

