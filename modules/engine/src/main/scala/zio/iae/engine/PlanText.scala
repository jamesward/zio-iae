package zio.iae.engine

import zio.iae.*
import zio.iae.engine.policy.{Decision, PlanDecision}

/** Plans as text for people: the CLI's output and, later, Slack approval requests. */
object PlanText:
  def render(plan: Plan): String =
    val header = s"${plan.phase} plan ${plan.hash.short} for ${plan.app.value}/${plan.env.value}" +
      plan.target.fold(" (no commit)")(c => s" at ${c.short}")
    if plan.isEmpty then s"$header: nothing to change"
    else (header :: plan.changes.map(line)).mkString("\n")

  def render(decision: PlanDecision): String =
    val reasons = decision.changes.flatMap(c => c.reasons.map(r => s"  ${c.change.value}: $r"))
    (s"policy: ${describe(decision.decision)}" :: reasons).mkString("\n")

  def describe(decision: Decision): String = decision match
    case Decision.Auto                   => "can go ahead"
    case Decision.Deny                   => "denied"
    case Decision.Approve(count, groups) => s"needs $count approval(s) from ${groups.toList.sorted.map("@" + _.handle).mkString(", ")}"

  private def line(planned: PlannedChange): String =
    val target = s"${planned.address.kind} ${planned.address.id.value}"
    planned.change match
      case Change.Create(_, _) => s"  + create  $target [${planned.risk}]"
      case Change.Update(_, _, fields) =>
        (s"  ~ update  $target [${planned.risk}]" :: fields.toList.map(f => s"      ${f.field}: ${f.from} -> ${f.to}")).mkString("\n")
      case Change.Orphan(_, since) => s"  - orphan  $target [${planned.risk}] (tagged iae:orphaned=$since, not deleted)"
      case Change.Delete(_)        => s"  ! delete  $target [${planned.risk}]"
