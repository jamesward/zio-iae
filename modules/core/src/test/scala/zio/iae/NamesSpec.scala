package zio.iae

import zio.test.*

object NamesSpec extends ZIOSpecDefault:
  def spec = suite("Names")(
    test("accepts lowercase names with single hyphens"):
      assertTrue(
        LogicalId.from("uploads").isRight,
        LogicalId.from("user-avatars-2").isRight,
        AppName.from("photo-app").isRight,
        EnvName.from("prod").isRight,
      )
    ,
    test("rejects names that can't be part of a physical resource name"):
      assertTrue(
        LogicalId.from("").isLeft,
        LogicalId.from("Uploads").isLeft,
        LogicalId.from("2fast").isLeft,
        LogicalId.from("double--hyphen").isLeft,
        LogicalId.from("trailing-").isLeft,
        LogicalId.from("under_score").isLeft,
        EnvName.from("much-too-long-env").isLeft,
      )
    ,
    test("literal names are checked at compile time"):
      for
        good <- typeCheck("""LogicalId("uploads")""")
        bad  <- typeCheck("""LogicalId("Uploads")""")
        dyn  <- typeCheck("""val s = "uploads"; LogicalId(s)""")
      yield assertTrue(good.isRight, bad.isLeft, dyn.isLeft)
    ,
  )
