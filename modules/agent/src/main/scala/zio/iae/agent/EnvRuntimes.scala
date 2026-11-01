package zio.iae.agent

import zio.*
import zio.http.Client
import zio.iae.*
import zio.iae.aws.BucketNaming
import zio.iae.aws.client.*
import zio.iae.engine.*

object EnvRuntimes:
  /** An env's engine. With an account, it plans as the account's planner role and applies as its deployer role,
    * both assumed from `base` (the agent's own credentials); without one, `base` does both (a single account).
    * Building it checks that the roles can be assumed.
    */
  def make(
      env: EnvName,
      account: Option[EnvAccount],
      base: AwsConfig,
      naming: BucketNaming,
      workspace: GitWorkspace,
      auditEndpoint: Option[Endpoint] = None,
  ): ZIO[Client & Scope, AgentError, EnvRuntime] =
    def failed(e: AwsError) = AgentError.Config(s"env ${env.value}: ${e.message}")
    for
      http   <- ZIO.service[Client]
      region <- ZIO.foreach(account.flatMap(_.region))(r => ZIO.fromEither(Region.from(r)).mapError(AgentError.Config(_)))
      configs <- account match
                   case None => ZIO.succeed((base, base))
                   case Some(a) =>
                     StsClient.assumed(base, http, a.planner, s"iae-plan-${env.value}", region)
                       .zip(StsClient.assumed(base, http, a.deployer, s"iae-deploy-${env.value}", region))
      planner  <- AwsEngine.clientsFor(configs._1).mapError(failed)
      deployer <- if account.isEmpty then ZIO.succeed(planner) else AwsEngine.clientsFor(configs._2).mapError(failed)
      // Evidence is written with the deployer's credentials, into the env's account.
      evidence = account.flatMap(_.audit).map: audit =>
                   val auditConfig = auditEndpoint.fold(configs._2)(e => configs._2.copy(endpoint = Some(e)))
                   EvidenceRecorder(S3Client(http, configs._2), AuditManagerClient(http, auditConfig), audit)
    yield EnvRuntime(env, AwsEngine.make(naming, planner, deployer, GitCommitOrder(workspace.dir)), workspace, evidence)
