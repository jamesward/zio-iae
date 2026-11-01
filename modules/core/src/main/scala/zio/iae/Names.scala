package zio.iae

import scala.quoted.*
import zio.schema.Schema

enum NameKind derives CanEqual:
  case LogicalId, AppName, EnvName

  def maxLength: Int = this match
    case NameKind.LogicalId => 40
    case NameKind.AppName   => 20
    case NameKind.EnvName   => 12

final case class InvalidName(kind: NameKind, value: String, reason: String) derives CanEqual:
  def message: String = s"invalid $kind '$value': $reason"

/** Rules shared by every name that ends up in a physical resource name: lowercase letters, digits and single
  * hyphens, starting with a letter.
  */
object NameRules:
  private val pattern = "[a-z][a-z0-9]*(-[a-z0-9]+)*".r

  def check(kind: NameKind, value: String): Either[InvalidName, String] =
    if value.isEmpty then Left(InvalidName(kind, value, "must not be empty"))
    else if value.length > kind.maxLength then
      Left(InvalidName(kind, value, s"must be at most ${kind.maxLength} characters"))
    else if !pattern.matches(value) then
      Left(InvalidName(kind, value, "must be lowercase letters, digits and single hyphens, starting with a letter"))
    else Right(value)

  def literal(kind: NameKind, value: Expr[String])(using Quotes): Expr[String] =
    import quotes.reflect.report
    value.value match
      case Some(v) =>
        check(kind, v) match
          case Right(valid) => Expr(valid)
          case Left(error)  => report.errorAndAbort(error.message)
      case None =>
        report.errorAndAbort(s"$kind(...) needs a string literal; use $kind.from for values known only at runtime")

  def schema[A](kind: NameKind, wrap: String => A, unwrap: A => String): Schema[A] =
    Schema[String].transformOrFail(s => check(kind, s).map(wrap).left.map(_.message), a => Right(unwrap(a)))

opaque type LogicalId = String

object LogicalId:
  inline def apply(inline value: String): LogicalId = ${ literal('value) }

  def from(value: String): Either[InvalidName, LogicalId] = NameRules.check(NameKind.LogicalId, value)

  private def literal(value: Expr[String])(using Quotes): Expr[LogicalId] = NameRules.literal(NameKind.LogicalId, value)

  extension (id: LogicalId) def value: String = id

  given CanEqual[LogicalId, LogicalId] = CanEqual.derived
  given Ordering[LogicalId]            = Ordering.String
  given Schema[LogicalId]              = NameRules.schema(NameKind.LogicalId, identity, identity)

opaque type AppName = String

object AppName:
  inline def apply(inline value: String): AppName = ${ literal('value) }

  def from(value: String): Either[InvalidName, AppName] = NameRules.check(NameKind.AppName, value)

  private def literal(value: Expr[String])(using Quotes): Expr[AppName] = NameRules.literal(NameKind.AppName, value)

  extension (name: AppName) def value: String = name

  given CanEqual[AppName, AppName] = CanEqual.derived
  given Schema[AppName]            = NameRules.schema(NameKind.AppName, identity, identity)

opaque type EnvName = String

object EnvName:
  inline def apply(inline value: String): EnvName = ${ literal('value) }

  def from(value: String): Either[InvalidName, EnvName] = NameRules.check(NameKind.EnvName, value)

  private def literal(value: Expr[String])(using Quotes): Expr[EnvName] = NameRules.literal(NameKind.EnvName, value)

  extension (name: EnvName) def value: String = name

  given CanEqual[EnvName, EnvName] = CanEqual.derived
  given Schema[EnvName]            = NameRules.schema(NameKind.EnvName, identity, identity)
