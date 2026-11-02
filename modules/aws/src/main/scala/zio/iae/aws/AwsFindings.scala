package zio.iae.aws

import zio.*
import zio.iae.*
import zio.iae.aws.client.*

/** The names a managed resource has in AWS, for matching what security services say about it. */
object PhysicalNames:
  def of(address: Address, buckets: BucketNaming): List[String] = address.kind match
    case ResourceKind.Bucket  => buckets.physical(address).toOption.map(_.value).toList
    case ResourceKind.Queue   => QueueNaming.physical(address).toOption.toList
    case ResourceKind.Table   => List(TableNaming.physical(address))
    case ResourceKind.Topic   => List(TopicNaming.physical(address))
    case ResourceKind.Secret  => List(SecretNaming.physical(address))
    case ResourceKind.Role    => RoleProvider.roleName(address.app, address.env).toOption.map(_.value).toList
    case ResourceKind.Service =>
      List(ServiceNaming.loadBalancer(address)) ++ Color.values.toList.map(ServiceNaming.ecsService(address, _))

/** Checks the engine runs itself on the managed resources, from their configuration:
  *   - a bucket without S3 Block Public Access fully on (AWS turns it on for new buckets, so off means someone
  *     turned it off): high;
  *   - a queue whose messages aren't encrypted at rest (neither SQS-managed nor a KMS key): medium;
  *   - a service whose load balancer serves plain HTTP rather than TLS: medium. `ServiceSpec` has no certificate
  *     yet, so this is reported until it does.
  */
final case class ConfigChecks(s3: S3Client, sqs: SqsClient, elb: Elbv2Client, buckets: BucketNaming):
  def check(address: Address): IO[AwsError, List[SecurityFinding]] =
    def finding(severity: Severity, title: String, detail: String) =
      SecurityFinding("iae", s"iae/${address.show}/${title.takeWhile(_ != ' ')}", severity, title, Some(address), detail, threat = false)
    address.kind match
      case ResourceKind.Bucket =>
        ZIO.foreach(buckets.physical(address).toOption): bucket =>
          s3.publicAccessBlocked(bucket).mapError(e => AwsError.InvalidRequest(e.message)).map: blocked =>
            Option.unless(blocked)(finding(Severity.High, "public access isn't blocked", s"S3 Block Public Access isn't fully on for `${bucket.value}`.")).toList
        .map(_.toList.flatten)
      case ResourceKind.Queue =>
        ZIO.foreach(QueueNaming.physical(address).toOption): name =>
          sqs.queueUrl(name).flatMap(ZIO.foreach(_)(sqs.attributes)).map: attributes =>
            attributes.toList.flatMap: a =>
              val encrypted = a.get("SqsManagedSseEnabled").contains("true") || a.get("KmsMasterKeyId").exists(_.nonEmpty)
              Option.unless(encrypted)(finding(Severity.Medium, "messages aren't encrypted at rest", s"`$name` has neither SQS-managed encryption nor a KMS key.")).toList
        .map(_.toList.flatten)
      case ResourceKind.Service =>
        elb.loadBalancer(ServiceNaming.loadBalancer(address)).flatMap: lb =>
          ZIO.foreach(lb)(l => elb.listeners(l.arn)).map: listeners =>
            listeners.getOrElse(Nil).filter(_.protocol == "HTTP").map: l =>
              finding(Severity.Medium, "plain HTTP, no TLS", s"Its load balancer listens on port ${l.port} over HTTP. `ServiceSpec` can't set a certificate yet.")
      case _ => ZIO.succeed(Nil)

/** Security findings about an app's managed resources in an env, from:
  *   - the engine's own configuration checks;
  *   - Security Hub, for findings on resources with the app's `iae:app` and `iae:env` tags (it collects other
  *     services' findings too; GuardDuty's at high severity are threats);
  *   - GuardDuty, for findings that name one of the app's resources; high severity is a threat;
  *   - IAM Access Analyzer, for resources of the app shared outside the account or publicly.
  *
  * A service that isn't enabled (or allowed) in the account is skipped, said in the log. A finding Security Hub
  * already has from another of them is kept once.
  */
final case class AwsFindings(
    discovery: Discovery,
    buckets: BucketNaming,
    checks: ConfigChecks,
    hub: SecurityHubClient,
    guardDuty: GuardDutyClient,
    analyzer: AccessAnalyzerClient,
) extends Findings:
  def security(app: AppName, env: EnvName): IO[ProviderError, List[SecurityFinding]] =
    discovery.managed(app, env).flatMap: found =>
      val live  = found.filter(_.orphanedSince.isEmpty).map(_.address)
      val names = found.map(_.address).flatMap(a => PhysicalNames.of(a, buckets).map(_ -> a))
      def about(texts: List[String]): Option[Address] = names.find((n, _) => texts.exists(_.contains(n))).map(_._2)
      def skipped[A](what: String)(effect: IO[AwsError, List[A]]): UIO[List[A]] =
        effect.catchAll(e => ZIO.logWarning(s"$what for ${app.value}/${env.value}: ${e.message}").as(Nil))
      for
        own <- ZIO.foreach(live)(a => skipped(s"checking ${a.show}")(checks.check(a))).map(_.flatten)
        fromHub <- skipped("reading Security Hub")(hub.findings(Map(TagKey.App.wire -> app.value, TagKey.Env.wire -> env.value))).map(_.flatMap: (product, f) =>
                     Severity.fromLabel(f.severity).map: severity =>
                       SecurityFinding(product, f.id, severity, f.title, about(f.resources :+ f.raw), f.description.take(500),
                         threat = product == "GuardDuty" && severity.atLeast(Severity.High)))
        fromGuardDuty <- skipped("reading GuardDuty")(guardDuty.findings).map(_.flatMap: f =>
                           for
                             severity <- Severity.fromLabel(f.severity)
                             address  <- about(List(f.raw))
                           yield SecurityFinding("GuardDuty", f.id, severity, f.title, Some(address), f.description.take(500), threat = severity.atLeast(Severity.High)))
        fromAnalyzer <- skipped("reading Access Analyzer")(analyzer.findings).map(_.flatMap: f =>
                          for
                            severity <- Severity.fromLabel(f.severity)
                            address  <- about(f.resources)
                          yield SecurityFinding("Access Analyzer", f.id, severity, f.title, Some(address), f.description, threat = false))
      yield
        val others = (fromGuardDuty ++ fromAnalyzer).filterNot(f => fromHub.exists(_.id.contains(f.id)))
        own ++ fromHub ++ others
