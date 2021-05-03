package dqv.vonneumann.dataqulaity.reconciler

object Main {

  trait Concat[A] {
    def ++(x: A, y: A): A
  }

  trait Eq[A] {
    def customEq(x: A, y: A): Boolean
  }

  implicit object IntEq extends Eq[Int] {
    override def customEq(x: Int, y: Int): Boolean = x == y
  }

  implicit class EqOperator[A: Eq](x: A) {
    def customEq(y: A): Boolean = implicitly[Eq[A]].customEq(x, y)
  }

  implicit object IntConcat extends Concat[Int] {
    override def ++(x: Int, y: Int): Int = (x.toString + y.toString).toInt
  }

  implicit class ConcatOperators[A: Concat](x: A) {
    def ++(y: A) = implicitly[Concat[A]].++(x, y)
  }

  def main(args: Array[String]): Unit = {
    val a = 1234
    val b = 765

    val c = a ++ b // Instance method from ConcatOperators — can be used with infix notation like other built-in "operators"

    println(c)

    val d = highOrderTest(a, b)(IntConcat.++) // 2-argument method from the typeclass instance

    println(a customEq b)
    // both calls to println print "1234765"
  }

  def highOrderTest[A](x: A, y: A)(fun: (A, A) => A) = fun(x, y)
}
