
object Combinators {

  /** Predicate AND that short-circuits with pure functions. */
  def andP[A](p: A => Boolean, q: A => Boolean): A => Boolean =
    a => p(a) && q(a)

  /** Predicate OR combinator. */
  def orP[A](p: A => Boolean, q: A => Boolean): A => Boolean =
    a => p(a) || q(a)

  /** pipe: forward function composition (Haskell's & / |>). */
  implicit final class PipeOps[A](private val a: A) extends AnyVal {
    def |>[B](f: A => B): B = f(a)
  }

  /** composeAll: left-to-right composition for readability. */
  def composeAll[A](fs: List[A => A]): A => A =
    fs.foldLeft((a: A) => a)((acc, f) => acc.andThen(f))


  // Validation type that accumulates errors
  type Errs = List[String]
  type V[A] = Either[Errs, A]

  def ok[A](a: A): V[A] = Right(a)
  def ko[A](e: String): V[A] = Left(List(e))

  def map2[A,B,C](va: V[A], vb: V[B])(f: (A,B) => C): V[C] =
    (va, vb) match {
      case (Right(a), Right(b)) => Right(f(a,b))
      case (Left(e1), Right(_)) => Left(e1)
      case (Right(_), Left(e2)) => Left(e2)
      case (Left(e1), Left(e2)) => Left(e1 ++ e2)
    }
  def map3[A,B,C,D](va: V[A], vb: V[B], vc: V[C])(f: (A,B,C) => D): V[D] =
    map2(map2(va, vb)((a,b) => (a,b)), vc){ case ((a,b), c) => f(a,b,c) }

}