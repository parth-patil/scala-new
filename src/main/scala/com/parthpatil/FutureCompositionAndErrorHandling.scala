package com.parthpatil

import scala.concurrent.{Await, Future}
import scala.util.{ Success, Failure }
import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class SimpleEcommerce(
  productService: ProductService,
  inventoryService: InventoryService,
  reviewService: ProductReviewService) {

  /**
   * Get ProductInfo by composing Futures with
   * for comprehension
   * @return
   */
  def getProductsToShow(): Future[Seq[ProductInfo]] = {

    for {
      products   <- productService.getProductListing()
      productIds = products map { _.id }

      // Spawn multiple parallel Futures
      futureMetadata  = Future.sequence( productIds map { productService.getProductMetadata } )
      futureInventory = Future.sequence( productIds map { inventoryService.getInventory } )
      futureReviews   = Future.sequence( productIds map { reviewService.getProductReviews } )

      // Wait for the Futures to complete
      metadata  <- futureMetadata
      inventory <- futureInventory
      reviews   <- futureReviews
    } yield {
      createProductInfo(
        productIds = productIds,
        metadata   = metadata,
        inventory  = inventory,
        reviews    = reviews.flatten
      )
    }
  }

  /**
   * same as above but using async await
   * @return
   */
  def getProductsToShow2(): Future[Seq[ProductInfo]] = {
    async {
      val products   = await { productService.getProductListing() }
      val productIds = products map { _.id }

      // Spawn multiple parallel Futures
      val futureMetadata  = Future.sequence( productIds map { productService.getProductMetadata } )
      val futureInventory = Future.sequence( productIds map { inventoryService.getInventory } )
      val futureReviews   = Future.sequence( productIds map { reviewService.getProductReviews } )

      // Wait for the Futures to complete
      val metadata  = await { futureMetadata }
      val inventory = await { futureInventory }
      val reviews   = await { futureReviews }

      createProductInfo(
        productIds = productIds,
        metadata   = metadata,
        inventory  = inventory,
        reviews    = reviews.flatten
      )
    }
  }

  def createProductInfo(
    productIds: Seq[String],
    metadata: Seq[ProductMetadata],
    inventory: Seq[Inventory],
    reviews: Seq[ProductReview]): Seq[ProductInfo] = {

    // Create lookup tables keyed by productId
    val metadataLookup  = metadata groupBy(_.productId) map { case(k, v) => k -> v.head }
    val inventoryLookup = inventory groupBy(_.productId) map { case(k, v) => k -> v.head }
    val reviewsLookup   = reviews.groupBy(_.productId)

    productIds map { id =>
      ProductInfo(
        productId = id,
        meta = metadataLookup.get(id),
        inventory = inventoryLookup.get(id),
        reviews = reviewsLookup.getOrElse(id, Seq[ProductReview]())
      )
    }
  }
}

case class Product(id: String, name: String)
case class ProductMetadata(productId: String, category: String, imageUrl: Option[String])
case class Inventory(productId: String, quantity: Long)
case class User(id: String, email: String)
case class ProductReview(
  id: String,
  productId: String,
  userId: String,
  created: Long,
  review: String,
  rating: Double)
case class ProductInfo(
  productId: String,
  meta: Option[ProductMetadata],
  inventory: Option[Inventory],
  reviews: Seq[ProductReview])

trait ProductService {
  def getProductListing(): Future[Seq[Product]]
  def getProductMetadata(productId: String): Future[ProductMetadata]
}

trait InventoryService {
  def getInventory(productId: String): Future[Inventory]
}

trait ProductReviewService {
  def getProductReviews(productId: String): Future[Seq[ProductReview]]
}

object SimpleEcommerce extends App {
  def test1(): Unit = {
    val f1 = Future { "foo" }
    val f2 = Future { "bar" }

    val foo = f1.andThen({
      case Success(r) => f2
      case Failure(e) => ""
    })

    val moo = Await.result(foo, 1 second)
    println(s"moo = $moo")
  }

  def andThenTest(): Unit = {
    val product: Future[Product] = ???
    product andThen {
      case Success(p) => // do something with the product
    } andThen {
      case Success(p) => // do yet another thing with the product
    }
  }

  def failedTest(): Unit = {
    val product: Future[Product] = ???
    def logException(e: Throwable) = ???
    val foo: Future[Throwable] = product.failed map { e: Throwable => logException(e); e }
  }

  def transformTest(): Unit = {
    val product: Future[Product] = ???
    def logException(e: Throwable) = ???
    val id: Future[String] = product.transform(
      {p: Product => p.id},
      {e: Throwable => logException(e)}
    )
  }

  /*
  def recoverTest(): Unit = {
    def getProductFromFailingDb(): Future[Product] = Future {
      throw new Exception("Db failure!")
    }

    def getProductFromWorkingDb(): Future[Product] = Future {
      Product(id="1", name="p1-working-db")
    }

    def getProductFromCache(): Product = Product(id="1", name="p1-cache")

    val p1: Future[Product] = Await.result(
      getProductFromFailingDb().recover {case e: Throwable => getProductFromCache() },
      1 second)

    val p2: Future[Product] = Await.result(
      getProductFromFailingDb().recoverWith {case e: Throwable => getProductFromWorkingDb() },
      1 second)

    println(s"foo1 = $foo1")
    println(s"foo2 = $foo2")
  }

  recoverTest()
  */
}
