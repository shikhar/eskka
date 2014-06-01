package eskka

import akka.actor._
import akka.util.Timeout

object Pinger {

  def props = Props(classOf[Pinger])

  case class PingRequest(id: String, from: ActorRef, to: Address, timeout: Timeout)

  sealed trait PingResponse {

    def request: PingRequest

  }

  case class PingOk(request: PingRequest) extends PingResponse

  case class PingTimeout(request: PingRequest) extends PingResponse

  private case class Ping(req: PingRequest)

}

class Pinger extends Actor {

  import Pinger._

  import context.dispatcher

  val pendingPings = collection.mutable.Map[PingRequest, Cancellable]()

  override def postStop() {
    pendingPings.values.foreach(_.cancel())
  }

  override def receive = {

    case req @ PingRequest(_, from, to, timeout) =>
      context.actorSelection(RootActorPath(to) / "user" / ActorNames.Pinger) ! Ping(req)
      val timeoutTask = context.system.scheduler.scheduleOnce(timeout.duration, self, PingTimeout(req))
      pendingPings += (req -> timeoutTask)

    case Ping(req) =>
      sender() ! Status.Success(req)

    case Status.Success(req: PingRequest) =>
      pendingPings.remove(req).foreach {
        timeoutTask =>
          timeoutTask.cancel()
          req.from ! PingOk(req)
      }

    case pt @ PingTimeout(req) =>
      pendingPings -= req
      req.from ! pt

  }

}
