package zio.iae.aws

import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
import java.util.HexFormat
import zio.*
import zio.iae.*
import zio.iae.aws.client.*

/** A service's physical names. Load balancer and target group names are limited to 32 characters, so they carry a
  * hash of the address; the ECS names are readable.
  */
object ServiceNaming:
  private def hash(address: Address): String =
    HexFormat.of().nn.formatHex(MessageDigest.getInstance("SHA-256").nn.digest(address.show.getBytes(UTF_8))).nn.take(12)

  def cluster(address: Address): String                = s"iae-${address.app.value}-${address.env.value}"
  def loadBalancer(address: Address): String           = s"iae-${hash(address)}"
  def targetGroup(address: Address, c: Color): String  = s"iae-${hash(address)}-${c.short}"
  // The env is in the name although clusters are per env: Floci keys services by name across clusters.
  def ecsService(address: Address, c: Color): String   = s"${address.env.value}-${address.id.value}-$c"
  def family(address: Address): String                 = s"iae-${address.app.value}-${address.env.value}-${address.id.value}"
  def alarmPrefix(address: Address, c: Color): String  = s"iae-${address.app.value}-${address.env.value}-${address.id.value}-$c-"
  def alarm(address: Address, c: Color): String        = s"${alarmPrefix(address, c)}5xx"

  /** The role ECS pulls images and writes logs as: the account's, made by bootstrap. */
  def executionRole(context: AwsContext): String = s"arn:aws:iam::${context.account.value}:role/iae/iae-task-execution"

/** The two sides of a blue/green deploy. */
enum Color derives CanEqual:
  case Blue, Green

  def other: Color = this match
    case Blue  => Green
    case Green => Blue

  def short: String = this match
    case Blue  => "b"
    case Green => "g"

  override def toString: String = this match
    case Blue  => "blue"
    case Green => "green"

object Color:
  def from(value: String): Option[Color] = values.find(_.toString == value)

final case class ObservedService(loadBalancer: LoadBalancer, tags: Map[String, String], blue: Option[TargetGroup], green: Option[TargetGroup]):
  def active: Color                = tags.get(ServiceProvider.ColorTag).flatMap(Color.from).getOrElse(Color.Blue)
  def failed: Option[String]       = tags.get(ServiceProvider.FailedTag)
  def group(c: Color): Option[TargetGroup] = c match
    case Color.Blue  => blue
    case Color.Green => green

/** An ECS service behind a load balancer, deployed blue/green by canary. The load balancer's tags say which color is
  * live and which spec it runs, so there's still no state file: a new image or new service settings change the spec
  * hash, and applying that change is the deploy. A deploy whose canary set off its alarm is rolled back and its spec
  * remembered (`iae:failed`), so it isn't retried; planning it again reports the failure instead, which keeps later
  * envs from getting it.
  */
final case class ServiceProvider(
    ecs: EcsClient,
    elb: Elbv2Client,
    cloudwatch: CloudWatchClient,
    network: UIO[Either[String, Network]],
    context: AwsContext,
    waitTimeout: Duration = 10.minutes,
) extends Provider[ServiceSpec, ObservedService]:
  import ServiceProvider.*

  val kind: ResourceKind = ResourceKind.Service

  def observe(address: Address): IO[ProviderError, Option[ObservedService]] =
    (for
      lb <- elb.loadBalancer(ServiceNaming.loadBalancer(address))
      observed <- ZIO.foreach(lb): lb =>
                    for
                      tags  <- elb.tags(lb.arn)
                      blue  <- elb.targetGroup(ServiceNaming.targetGroup(address, Color.Blue))
                      green <- elb.targetGroup(ServiceNaming.targetGroup(address, Color.Green))
                    yield ObservedService(lb, tags, blue, green)
    yield observed).mapError(failed(address, "observe"))

  def diff(address: Address, desired: ServiceSpec, observed: Option[ObservedService]): Option[Change[ServiceSpec]] =
    observed match
      case None => Some(Change.Create(address, desired))
      case Some(current) =>
        val hash    = SpecHash.of(ResourceSpec.Service(desired))
        val tags    = ManagedTags.differences(current.tags.filterNot((k, _) => k == ColorTag || k == FailedTag), ManagedTags.identity(address, hash))
        val version = Option.when(ManagedTags.specHash(current.tags).forall(_ != hash)):
          FieldChange("version", current.tags.getOrElse(ImageTag, "(none)"), desired.image.getOrElse("(none)"))
        NonEmptyChunk.fromIterableOption(version.toList ++ tags.filterNot(_.field == s"tag ${TagKey.Spec.wire}")).map(Change.Update(address, desired, _))

  def assess(change: Change[ServiceSpec], observed: Option[ObservedService]): Assessment = change match
    case Change.Create(_, _)    => Assessment(Risk.Low, Set.empty)
    case Change.Update(_, _, _) => Assessment(Risk.Low, Set.empty) // a deploy, gated by its canary
    case Change.Orphan(_, _)    => Assessment(Risk.Low, Set.empty)
    case Change.Delete(_)       => Assessment(Risk.High, Set.empty)

  def applyChange(change: Change[ServiceSpec], ctx: ApplyContext): IO[ProviderError, Unit] =
    val address = change.address
    change match
      case Change.Create(_, spec) =>
        for
          _       <- create(address, spec, ctx)
          current <- existing(address)
          _       <- ZIO.foreachDiscard(spec.image)(_ => deploy(address, spec, current, canary = false, ctx))
        yield ()
      case Change.Update(_, spec, _) =>
        existing(address).flatMap: current =>
          val hash = SpecHash.of(ResourceSpec.Service(spec))
          if current.failed.contains(hash.value) then
            ZIO.fail(ProviderError(address, "deploy", s"${spec.image.getOrElse("this version")} was rolled back by its canary; push a fix"))
          else if spec.image.isEmpty then tag(address, current, Map(TagKey.Spec.wire -> hash.value), ctx)
          else deploy(address, spec, current, canary = true, ctx)
      case Change.Orphan(_, since) =>
        existing(address).flatMap(current => elb.tag(current.loadBalancer.arn, ManagedTags.orphaned(current.tags, since, ctx)).mapError(failed(address, "tag as orphaned")))
      case Change.Delete(_) => ZIO.fail(ProviderError(address, "delete", "services are orphaned, not deleted"))

  private def create(address: Address, spec: ServiceSpec, ctx: ApplyContext): IO[ProviderError, Unit] =
    val tags = ManagedTags.converged(Map.empty, address, SpecHash.of(ResourceSpec.Service(spec.copy(image = None))), ctx)
    (for
      net     <- network.flatMap(ZIO.fromEither(_)).mapError(AwsError.InvalidRequest(_))
      cluster  = ServiceNaming.cluster(address)
      exists  <- ecs.clusterExists(cluster)
      _       <- ecs.createCluster(cluster, Map(TagKey.Managed.wire -> "true")).unless(exists)
      lb      <- elb.loadBalancer(ServiceNaming.loadBalancer(address)).someOrElseZIO(elb.createLoadBalancer(ServiceNaming.loadBalancer(address), net.subnets, tags))
      groups <- ZIO.foreach(Color.values.toList): c =>
                  elb.targetGroup(ServiceNaming.targetGroup(address, c)).someOrElseZIO:
                    elb.createTargetGroup(ServiceNaming.targetGroup(address, c), spec.port, net.vpc, spec.healthPath, Map(TagKey.Managed.wire -> "true"))
      listeners <- elb.listeners(lb.arn)
      _ <- elb.createListener(lb.arn, spec.listenerPort, Map(groups(0).arn -> 100, groups(1).arn -> 0)).when(listeners.isEmpty)
      _ <- elb.tag(lb.arn, tags + (ColorTag -> Color.Blue.toString))
    yield ()).mapError(failed(address, "create"))

  /** Starts the spec on the idle color, moves the traffic over by the canary's steps while its alarm stays quiet, and
    * retires the old color; on an alarm, puts all traffic back and remembers the version as failed.
    */
  private def deploy(address: Address, spec: ServiceSpec, current: ObservedService, canary: Boolean, ctx: ApplyContext): IO[ProviderError, Unit] =
    val live  = current.active
    val next  = if canary then live.other else live
    val hash  = SpecHash.of(ResourceSpec.Service(spec))
    val image = spec.image.getOrElse("")
    (for
      net       <- network.flatMap(ZIO.fromEither(_)).mapError(AwsError.InvalidRequest(_))
      cluster    = ServiceNaming.cluster(address)
      liveGroup <- ZIO.fromOption(current.group(live)).orElseFail(AwsError.InvalidRequest(s"no $live target group"))
      nextGroup <- ZIO.fromOption(current.group(next)).orElseFail(AwsError.InvalidRequest(s"no $next target group"))
      role       = s"arn:aws:iam::${context.account.value}:role/iae/iae-${address.app.value}-${address.env.value}"
      taskDef   <- ecs.registerTaskDefinition(ServiceNaming.family(address), ContainerSpec("app", image, spec.port, spec.environment ++ ServiceProvider.metricsEnvironment(address, next), spec.cpu, spec.memory, spec.command), Some(role), Some(ServiceNaming.executionRole(context)))
      name       = ServiceNaming.ecsService(address, next)
      // Armed before the new tasks start, so the alarms have settled by the first bake.
      gates      = ServiceProvider.gates(address, next, current.loadBalancer, nextGroup, spec.canary)
      _         <- armed(address, next, gates, image)
      existing  <- ecs.service(cluster, name)
      _         <- existing match
                     case None    => ecs.createService(cluster, name, taskDef, spec.count, net.subnets, nextGroup.arn, "app", spec.port, Map(TagKey.Managed.wire -> "true"))
                     case Some(_) => ecs.updateService(cluster, name, Some(taskDef), Some(spec.count))
      _         <- running(cluster, name, nextGroup, spec.count)
      listener  <- elb.listeners(current.loadBalancer.arn).flatMap(ls => ZIO.fromOption(ls.headOption).orElseFail(AwsError.InvalidRequest("no listener")))
      outcome   <- if !canary then elb.setWeights(listener.arn, Map(nextGroup.arn -> 100, liveGroup.arn -> 0)).as(None)
                   else steps(address, spec, listener, nextGroup, liveGroup, next, gates)
      _ <- outcome match
             case Some(why) =>
               for
                 _ <- elb.setWeights(listener.arn, Map(liveGroup.arn -> 100, nextGroup.arn -> 0))
                 _ <- ecs.updateService(cluster, name, None, Some(0))
                 _ <- elb.tag(current.loadBalancer.arn, Map(FailedTag -> hash.value))
                 _ <- ZIO.fail(AwsError.InvalidRequest(s"rolled back $image: $why"))
               yield ()
             case None =>
               for
                 old <- ecs.service(cluster, ServiceNaming.ecsService(address, live))
                 _   <- ecs.updateService(cluster, ServiceNaming.ecsService(address, live), None, Some(0)).when(canary && old.isDefined)
                 _ <- elb.tag(current.loadBalancer.arn, ManagedTags.converged(current.tags, address, hash, ctx) ++ Map(ColorTag -> next.toString, ImageTag -> image))
               yield ()
    yield ()).mapError:
      case AwsError.InvalidRequest(detail) => ProviderError(address, "deploy", detail) // a rollback or a precondition
      case other                           => ProviderError(address, "deploy", other.message)

  /** Each step's share of the traffic, then its bake; None if every step stayed quiet (and the new color then has all
    * the traffic), or why it has to roll back.
    */
  private def steps(address: Address, spec: ServiceSpec, listener: Listener, next: TargetGroup, live: TargetGroup, color: Color, gates: List[Gate])
      : IO[AwsError, Option[String]] =
    val shares = spec.canary.steps.map(_.max(0).min(100))
    ZIO
      .foldLeft(shares)(Option.empty[String]): (rolledBack, share) =>
        if rolledBack.isDefined then ZIO.succeed(rolledBack)
        else
          for
            _      <- elb.setWeights(listener.arn, Map(next.arn -> share, live.arn -> (100 - share)))
            _      <- ZIO.logInfo(s"${address.show}: $share% on $color, baking ${spec.canary.bakeSeconds}s")
            _      <- Clock.sleep(spec.canary.bakeSeconds.seconds)
            states <- cloudwatch.states(gates.map(_.alarm.name))
          yield gates.find(g => states.get(g.alarm.name).contains(AlarmState.Alarm)).map(g => s"its ${g.describe} alarm went off at $share%")
      .tap(result => elb.setWeights(listener.arn, Map(next.arn -> 100, live.arn -> 0)).when(result.isEmpty && !shares.lastOption.contains(100)))

  /** Real time: waits on ECS and the target group, whatever the agent's clock. */
  private def running(cluster: String, name: String, group: TargetGroup, count: Int): IO[AwsError, Unit] =
    (ecs.service(cluster, name).zip(elb.health(group.arn)))
      .repeat(Schedule.spaced(1.second) && Schedule.recurUntil[(Option[EcsService], List[TargetHealth])]((s, t) => s.exists(_.runningCount >= count) && t.size >= count))
      .timeoutFail(AwsError.InvalidRequest(s"$name didn't start $count task(s) within ${waitTimeout.render}"))(waitTimeout)
      .withClock(Clock.ClockLive)
      .unit

  /** Writes the color's gates as the spec has them, removes the ones it no longer has, and starts them all quiet. */
  private def armed(address: Address, color: Color, gates: List[Gate], image: String): IO[AwsError, Unit] =
    for
      _     <- ZIO.foreachDiscard(gates)(g => cloudwatch.putAlarm(g.alarm))
      names <- cloudwatch.alarmNames(ServiceNaming.alarmPrefix(address, color))
      _     <- cloudwatch.deleteAlarms(names.filterNot(gates.map(_.alarm.name).contains))
      _     <- ZIO.foreachDiscard(gates)(g => cloudwatch.setState(g.alarm.name, AlarmState.Ok, s"deploying $image"))
    yield ()

  private def tag(address: Address, current: ObservedService, tags: Map[String, String], ctx: ApplyContext): IO[ProviderError, Unit] =
    elb.tag(current.loadBalancer.arn, ManagedTags.converged(current.tags, address, SpecHash.from(tags(TagKey.Spec.wire)).get, ctx)).mapError(failed(address, "tag"))

  private def existing(address: Address): IO[ProviderError, ObservedService] =
    observe(address).someOrFail(ProviderError(address, "observe", s"load balancer ${ServiceNaming.loadBalancer(address)} does not exist"))

  private def failed(address: Address, operation: String)(error: AwsError): ProviderError = ProviderError(address, operation, error.message)

/** One of a color's canary alarms, and how a rollback names it. */
final case class Gate(alarm: MetricAlarm, describe: String)

object ServiceProvider:
  /** The alarms that gate a color's canary: its 5xx count, its response time if the spec has a latency gate, and the
    * app's own alarms on its metrics for that color.
    */
  def gates(address: Address, color: Color, lb: LoadBalancer, group: TargetGroup, canary: CanarySpec): List[Gate] =
    val prefix = ServiceNaming.alarmPrefix(address, color)
    val period = canary.alarmPeriodSeconds
    val elbDimensions = Map("LoadBalancer" -> arnSuffix(lb.arn, "loadbalancer/"), "TargetGroup" -> arnSuffix(group.arn, ""))
    val errors = Gate(
      MetricAlarm(s"${prefix}5xx", "AWS/ApplicationELB", "HTTPCode_Target_5XX_Count", elbDimensions, "Sum", period, 1, canary.maxErrors.toDouble, "GreaterThanOrEqualToThreshold"),
      "5xx",
    )
    val latency = canary.latency.map: gate =>
      Gate(
        MetricAlarm(s"${prefix}latency", "AWS/ApplicationELB", "TargetResponseTime", elbDimensions, gate.statistic, period, 1, gate.millis / 1000.0, "GreaterThanThreshold"),
        s"latency (${gate.statistic} over ${gate.millis} ms)",
      )
    val own = canary.alarms.map: a =>
      Gate(
        MetricAlarm(
          s"${prefix}app-${a.name}",
          Service.metricsNamespace(address.app),
          a.metric,
          Map("Service" -> address.id.value, "Color" -> color.toString),
          a.statistic,
          period,
          1,
          a.threshold,
          if a.below then "LessThanOrEqualToThreshold" else "GreaterThanOrEqualToThreshold",
        ),
        a.name,
      )
    errors :: latency.toList ++ own

  /** Where the app's containers publish their own metrics, and as which version. */
  def metricsEnvironment(address: Address, color: Color): Map[String, String] =
    Map("IAE_METRICS_NAMESPACE" -> Service.metricsNamespace(address.app), "IAE_SERVICE" -> address.id.value, "IAE_COLOR" -> color.toString)

  val ColorTag  = "iae:color"
  val ImageTag  = "iae:image"
  val FailedTag = "iae:failed"

  /** CloudWatch's dimension values: the part of the ARN after `loadbalancer/` for a load balancer, and after the
    * account for a target group (`targetgroup/name/id`).
    */
  def arnSuffix(arn: String, after: String): String =
    val resource = arn.split(':').lastOption.getOrElse(arn)
    if after.isEmpty then resource else resource.stripPrefix(after)

/** The alarms of each managed service's live color, found through discovery. */
final case class ServiceWatch(provider: ServiceProvider, discovery: Discovery) extends Watch:
  def alarms(app: AppName, env: EnvName): IO[ProviderError, List[ServiceAlarm]] =
    discovery.managed(app, env).flatMap: found =>
      ZIO.foreach(found.filter(_.address.kind == ResourceKind.Service))(d => live(d.address, d.commit)).map(_.flatten)

  private def live(address: Address, commit: Option[CommitSha]): IO[ProviderError, List[ServiceAlarm]] =
    provider.observe(address).flatMap:
      case None => ZIO.succeed(Nil)
      case Some(current) =>
        val prefix = ServiceNaming.alarmPrefix(address, current.active)
        (for
          names  <- provider.cloudwatch.alarmNames(prefix)
          states <- provider.cloudwatch.states(names)
        yield names.sorted.map: name =>
          val gate = name.stripPrefix(prefix) match
            case s if s.startsWith("app-") => s.stripPrefix("app-")
            case s                         => s
          ServiceAlarm(address, name, gate, states.get(name).contains(AlarmState.Alarm), current.tags.get(ServiceProvider.ImageTag), commit)
        ).mapError(e => ProviderError(address, "watch", e.message))

final case class ServiceDriver(provider: ServiceProvider) extends ResourceDriver:
  val kind: ResourceKind = ResourceKind.Service

  def plan(address: Address, desired: ResourceSpec): IO[ProviderError, Option[PlannedChange]] =
    desired match
      case ResourceSpec.Service(spec) =>
        provider.observe(address).map: observed =>
          provider.diff(address, spec, observed).map: change =>
            val assessment = provider.assess(change, observed)
            PlannedChange(change.map(ResourceSpec.Service(_)), assessment.risk, assessment.flags)
      case other => ZIO.fail(ProviderError(address, "plan", s"expected a service spec, got ${other.kind}"))

  def apply(change: Change[ResourceSpec], context: ApplyContext): IO[ProviderError, Unit] =
    val typed: Either[ProviderError, Change[ServiceSpec]] = change match
      case Change.Create(address, ResourceSpec.Service(spec))         => Right(Change.Create(address, spec))
      case Change.Update(address, ResourceSpec.Service(spec), fields) => Right(Change.Update(address, spec, fields))
      case Change.Orphan(address, since)                              => Right(Change.Orphan(address, since))
      case Change.Delete(address)                                     => Right(Change.Delete(address))
      case other                                                      => Left(ProviderError(other.address, "apply", "expected a service change"))
    ZIO.fromEither(typed).flatMap(provider.applyChange(_, context))

  def lastCommit(address: Address): IO[ProviderError, Option[CommitSha]] =
    provider.observe(address).map(_.flatMap(o => ManagedTags.commit(o.tags)))
