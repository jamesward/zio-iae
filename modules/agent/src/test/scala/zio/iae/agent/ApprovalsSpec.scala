package zio.iae.agent

import zio.*
import zio.iae.*
import zio.iae.engine.*
import zio.iae.engine.policy.*
import zio.test.*

/** The life of approval requests in Slack: reminders, expiry, superseding and the label after an apply. */
object ApprovalsSpec extends ZIOSpecDefault:
  val approvers = SlackGroup.from("infra-approvers").toOption.get
  val approve: Decision.Approve = Decision.Approve(1, Set(approvers))
  val channel   = "C_PHOTOS"

  def plan(id: String): Plan =
    val address = Address(AppName("photo-app"), EnvName("prod"), ResourceKind.Bucket, LogicalId.from(id).toOption.get)
    Plan(AppName("photo-app"), EnvName("prod"), None, ContentHash.of(id), Phase.Expand, List(PlannedChange(Change.Create(address, ResourceSpec.Bucket(BucketSpec())), Risk.High, Set.empty)))

  def decision(p: Plan): PlanDecision = PlanDecision(approve, Nil)

  val setup: UIO[(InMemorySlack, Approvals)] =
    InMemorySlack.make(Map("infra-approvers" -> Set("U_ALICE"))).map(s => (s, Approvals(s, s.self, channel, 24.hours)))

  def spec = suite("Approvals")(
    test("an unanswered request gets one reminder after an hour, mentioning the group"):
      for
        (slack, approvals) <- setup
        first   <- approvals.status(plan("a"), decision(plan("a")), approve, None)
        ts       = first match { case ApprovalStatus.Pending(ts) => ts; case _ => "" }
        _       <- approvals.status(plan("a"), decision(plan("a")), approve, None)
        early   <- slack.threadTexts(channel, ts)
        _       <- TestClock.adjust(61.minutes)
        _       <- approvals.status(plan("a"), decision(plan("a")), approve, None).repeatN(2)
        later   <- slack.threadTexts(channel, ts)
        request <- slack.posted(channel).map(_.head)
      yield assertTrue(
        early.isEmpty,
        later.size == 1,
        later.head.startsWith("<!subteam^S_infra-approvers>"),
        request.text.contains(Approvals.reminded),
        request.request.isDefined,
      ),
    test("a request for a different plan supersedes the open one, which then covers nothing"):
      for
        (slack, approvals) <- setup
        _      <- approvals.status(plan("a"), decision(plan("a")), approve, None)
        _      <- approvals.status(plan("b"), decision(plan("b")), approve, None)
        posted <- slack.posted(channel)
        back   <- approvals.status(plan("a"), decision(plan("a")), approve, None)
        all    <- slack.posted(channel)
      yield assertTrue(
        posted.map(_.text).zip(List(Approvals.Superseded, Approvals.Open)).forall(_.startsWith(_)) && posted.size == 2,
        back.isInstanceOf[ApprovalStatus.Pending],
        all.size == 3,
      ),
    test("an expired request is labelled and a new one is posted"):
      for
        (slack, approvals) <- setup
        _      <- approvals.status(plan("a"), decision(plan("a")), approve, None)
        _      <- TestClock.adjust(25.hours)
        _      <- approvals.status(plan("a"), decision(plan("a")), approve, None)
        posted <- slack.posted(channel)
      yield assertTrue(posted.size == 2, posted.map(_.text).zip(List(Approvals.Expired, Approvals.Open)).forall(_.startsWith(_))),
    test("an approved request is labelled once applied; a rejected one keeps its plan stopped"):
      for
        (slack, approvals) <- setup
        pending  <- approvals.status(plan("a"), decision(plan("a")), approve, None)
        ts        = pending match { case ApprovalStatus.Pending(ts) => ts; case _ => "" }
        _        <- slack.react(ts, "U_ALICE", Approvals.approveReaction)
        approved <- approvals.status(plan("a"), decision(plan("a")), approve, None)
        _        <- approved match
                      case ApprovalStatus.Approved(_, request) => approvals.applied(request)
                      case _                                   => ZIO.unit
        labelled <- slack.posted(channel).map(_.head.text)
        p2       <- approvals.status(plan("b"), decision(plan("b")), approve, None)
        ts2       = p2 match { case ApprovalStatus.Pending(ts) => ts; case _ => "" }
        _        <- slack.react(ts2, "U_ALICE", Approvals.rejectReaction)
        rejected <- approvals.status(plan("b"), decision(plan("b")), approve, None)
        again    <- approvals.status(plan("b"), decision(plan("b")), approve, None)
        posted   <- slack.posted(channel)
      yield assertTrue(
        approved.isInstanceOf[ApprovalStatus.Approved],
        labelled.startsWith(Approvals.Approved),
        rejected.isInstanceOf[ApprovalStatus.Rejected],
        again.isInstanceOf[ApprovalStatus.Rejected],
        posted.size == 2,
        posted(1).text.startsWith(Approvals.Rejected),
      ),
  )
