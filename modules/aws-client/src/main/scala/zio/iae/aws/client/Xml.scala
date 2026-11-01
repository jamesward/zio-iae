package zio.iae.aws.client

import javax.xml.parsers.SAXParserFactory
import scala.util.Try
import scala.xml.{Elem, XML}

private[client] object Xml:
  def parse(text: String): Either[String, Elem] =
    Try:
      val factory = SAXParserFactory.newInstance().nn
      factory.setNamespaceAware(false)
      factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true)
      XML.withSAXParser(factory.newSAXParser().nn).loadString(text)
    .toEither.left.map(e => s"invalid XML: ${e.getMessage}")

  def escape(text: String): String =
    text.flatMap:
      case '&'  => "&amp;"
      case '<'  => "&lt;"
      case '>'  => "&gt;"
      case '"'  => "&quot;"
      case '\'' => "&apos;"
      case c    => c.toString

  def child(elem: Elem, label: String): Option[String] = (elem \ label).headOption.map(_.text)
