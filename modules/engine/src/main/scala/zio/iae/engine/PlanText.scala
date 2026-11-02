package zio.iae.engine

import zio.iae.*
import zio.iae.engine.policy.{Decision, PlanDecision}

/** Plans as text for people: the CLI's output and, later, Slack approval requests. */
object PlanText:
  def render(plan: Plan): String =
    val header = s"${plan.phase} plan ${plan.hash.short} for ${plan.app.value}/${plan.env.value}" +
      plan.target.fold(" (no commit)")(c => s" at ${c.short}")
    if plan.isEmpty then s"$header: nothing to change"
    else (header :: plan.changes.map(line) ++ plan.costDelta.map(d => s"  cost: ${money(d)} a month (projected)").toList).mkString("\n")

  /** A monthly cost change, signed: `+$43.20`, `-$1.50`, `$0.00`. */
  def money(delta: Double): String =
    val cents = math.round(delta * 100)
    if cents == 0 then "$0.00" else f"${if cents > 0 then "+" else "-"}$$${math.abs(cents) / 100d}%.2f"

  private def costOf(planned: PlannedChange): String =
    planned.cost.fold("")(c => s" ${money(c.delta)}/month" + (if c.exact then "" else " (est.)"))

  def render(decision: PlanDecision): String =
    val reasons = decision.changes.flatMap(c => c.reasons.map(r => s"  ${c.change.value}: $r"))
    (s"policy: ${describe(decision.decision)}" :: reasons).mkString("\n")

  def describe(decision: Decision): String = decision match
    case Decision.Auto                   => "can go ahead"
    case Decision.Deny                   => "denied"
    case Decision.Approve(count, groups) =>
      val names = groups.toList.sorted.map("@" + _.handle).mkString(", ")
      if groups.size > 1 then s"needs $count approval(s) from each of $names" else s"needs $count approval(s) from $names"

  def line(planned: PlannedChange): String =
    val target = s"${planned.address.kind} ${planned.address.id.value}"
    planned.change match
      case Change.Create(_, _) => s"  + create  $target [${planned.risk}]${costOf(planned)}"
      case Change.Update(_, _, fields) =>
        (s"  ~ update  $target [${planned.risk}]${costOf(planned)}" :: fields.toList.map(f => s"      ${f.field}: ${f.from} -> ${f.to}")).mkString("\n")
      case Change.Orphan(_, since) => s"  - orphan  $target [${planned.risk}] (tagged iae:orphaned=$since, not deleted)"
      case Change.Delete(_)        => s"  ! delete  $target [${planned.risk}]${costOf(planned)}"
      case Change.Prune(_, versions) =>
        s"  x prune   $target [${planned.risk}] (${versions.size} old version(s) nothing runs or rolls back to: ${versions.map(v => v.split('/').last).mkString(", ")})"
      case Change.Rollback(_, from, to) => s"  < rollback $target [${planned.risk}]\n      version: $from -> $to (marked failed, not redeployed)"
