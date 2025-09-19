
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
}