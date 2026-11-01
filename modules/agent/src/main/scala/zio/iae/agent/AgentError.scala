package zio.iae.agent

import zio.iae.engine.EngineError

enum AgentError derives CanEqual:
  case Git(detail: String)
  case Config(detail: String)
  case Manifest(app: String, detail: String)
  case Slack(detail: String)
  case Engine(error: EngineError)

  def message: String = this match
    case Git(detail)              => s"git: $detail"
    case Config(detail)           => s"configuration: $detail"
    case Manifest(app, detail)    => s"manifest of $app: $detail"
    case Slack(detail)            => s"slack: $detail"
    case Engine(error)            => error.message
