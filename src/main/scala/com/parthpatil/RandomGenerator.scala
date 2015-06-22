package com.parthpatil

object RandomGenerator extends App {
  trait Generator[+T] {
    self =>

    def generate: T

    def map[U](f: T => U): Generator[U] = new Generator[U] {
      def generate = f(self.generate)
    }

    def foreach(f: T => Unit): Unit = f(self.generate)

    def flatMap[U](f: T => Generator[U]): Generator[U] = new Generator[U] {
      def generate = f(self.generate).generate
    }
  }

  // Random integer generator
  val intGenerator = new Generator[Int] {
    val rand = new java.util.Random
    def generate = rand.nextInt
  }

  for {
    i <- intGenerator
  } println(i)

  Thread.sleep(10000)
  println("Done .....")
}
