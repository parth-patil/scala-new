package com.parthpatil

object S99 extends App {
  def findKthInList[A](ls: List[A], k: Int): Option[A] = {
    def loop(l: List[A], current: Int): Option[A] = {
      if (current > (k + 1))
        None
      else if (current == (k - 1))
        Some(l.head)
      else
        loop(l.tail, current + 1)
    }

    loop(ls, 0)
  }

  println("kth = " + findKthInList(List(1,2,3,4), 4))
}
