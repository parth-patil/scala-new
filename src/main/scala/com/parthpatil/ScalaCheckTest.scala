package com.parthpatil

import org.scalacheck._
import Prop.forAll

object ScalaCheckTest extends App {
  /*
  property("ListDoubleReverse") = forAll { l: List[Int] =>
    l.reverse.reverse == l
  }

  property("zipAndReverse") = forAll { (l1: List[Int], l2: List[Int]) =>
    // smart way to make the two lists of the same length!!
    val a = l1.take(l2.length)
    val b = l2.take(l1.length)
    (a.reverse zip b.reverse) == (a zip b).reverse
  }
  */

  /*
  val propConcatString = forAll { (s1: String, s2: String) =>
    (s1 + s2).endsWith(s2)
  }
  */

  /*
  trait Tree
  case class Node(left: Tree, right: Tree) extends Tree
  case class Leaf(x: Int) extends Tree

  val ints = Gen.choose(-100, 100)

  def leafs: Gen[Leaf] = for {
    x <- ints
  } yield Leaf(x)

  def nodes: Gen[Node] = for {
    left <- trees
    right <- trees
  } yield Node(left, right)

  def trees: Gen[Tree] = Gen.oneOf(leafs, nodes)

  def countries = Gen.oneOf("US", "CA", "CH", "IN", "UK")

  for {
    c <- countries
  } println(c)
  */
}

