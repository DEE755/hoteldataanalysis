sealed trait FError { def msg: String }
final case class ParseError(msg: String) extends FError
final case class DomainError(msg: String) extends FError

/** Very light validated-like type without external libs. */
object FEither {
  type V[A] = Either[List[FError], A]

  def ok[A](a: A): V[A] = Right(a)
  def ko[A](err: FError): V[A] = Left(List(err))

  /** Combine two validated values collecting errors. */
  def map2[A,B,C](va: V[A], vb: V[B])(f: (A,B) => C): V[C] =
    (va, vb) match {
      case (Right(a), Right(b))     => Right(f(a,b))
      case (Left(e1), Right(_))     => Left(e1)
      case (Right(_), Left(e2))     => Left(e2)
      case (Left(e1), Left(e2))     => Left(e1 ++ e2)
    }
}