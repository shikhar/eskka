package eskka

import java.util.UUID

import scala.Some
import scala.concurrent.{ Future, Promise }
import scala.concurrent.duration.{ Duration, FiniteDuration }
import scala.util.Success

import akka.actor._
import akka.cluster.{ Cluster, ClusterEvent, MemberStatus }
import akka.cluster.MemberStatus.{ Down, Exiting }
import akka.pattern.ask
import akka.util.Timeout

object QuorumBasedPartitionMonitor {

  def props(votingMembers: VotingMembers, evalDelay: FiniteDuration, pingTimeout: Timeout) =
    Props(classOf[QuorumBasedPartitionMonitor], votingMembers, evalDelay, pingTimeout)

  private val PingTimeoutReceiptFudge = 1.25

  private case class EnrollVoter(node: Address)

  private case class VoterRegistration(node: Address, ref: ActorRef)

  private case class Evaluate(node: Address)

  private case class EvaluateTimeout(node: Address, pollResults: Map[Address, Future[Pinger.PingResponse]])

}

class QuorumBasedPartitionMonitor(votingMembers: VotingMembers, evalDelay: FiniteDuration, pingTimeout: Timeout) extends Actor with ActorLogging {

  import QuorumBasedPartitionMonitor._

  import context.dispatcher

  private[this] val cluster = Cluster(context.system)

  private[this] var franchisedVoters: Set[Address] = Set.empty
  private[this] var registeredVoters: Map[Address, ActorRef] = Map.empty

  private[this] var unreachable: Set[Address] = Set.empty
  private[this] var pendingEval: Map[Address, (ActorRef, Cancellable)] = Map.empty

  private[this] val skipMemberStatus = Set[MemberStatus](Down, Exiting)

  require(votingMembers.addresses(cluster.selfAddress))

  override def preStart() {
    cluster.subscribe(self, ClusterEvent.InitialStateAsEvents, classOf[ClusterEvent.MemberEvent], classOf[ClusterEvent.ReachabilityEvent])
  }

  override def postStop() {
    cluster.unsubscribe(self)
    pendingEval.values.foreach(reap)
  }

  override def receive = {

    case EnrollVoter(node) if franchisedVoters contains node =>
      val pinger = RootActorPath(node) / "user" / ActorNames.Pinger
      val id = UUID.randomUUID
      implicit val timeout = pingTimeout
      (context.actorSelection(pinger) ? Identify(id)).mapTo[ActorIdentity] onComplete {
        case Success(ActorIdentity(i, Some(ref))) if i == id => self ! VoterRegistration(node, ref)
        case _ => self ! EnrollVoter(node)
      }

    case VoterRegistration(node, ref) if franchisedVoters contains node =>
      log.debug("registered [{}] as a voter at [{}]", node, ref)
      registeredVoters += (node -> ref)

    case mEvent: ClusterEvent.MemberEvent => mEvent match {

      case ClusterEvent.MemberUp(m) =>
        if (votingMembers.addresses(m.address)) {
          franchisedVoters += m.address
          self ! EnrollVoter(m.address)
        }

      case ClusterEvent.MemberExited(m) =>
        franchisedVoters -= m.address
        registeredVoters -= m.address
        forgetUnreachable(m.address)

      case ClusterEvent.MemberRemoved(m, _) =>
        franchisedVoters -= m.address
        registeredVoters -= m.address
        forgetUnreachable(m.address)

    }

    case rEvent: ClusterEvent.ReachabilityEvent => rEvent match {

      case ClusterEvent.UnreachableMember(m) =>
        if (!skipMemberStatus(m.status)) {
          unreachable += m.address
          evalAfterDelay(m.address, "unreachable")
        }

      case ClusterEvent.ReachableMember(m) =>
        forgetUnreachable(m.address)

    }

    case Evaluate(node) if (unreachable contains node) && !(pendingEval contains node) =>
      val promises = registeredVoters.mapValues(_ => Promise[Pinger.PingResponse]()).view.force
      val collector = pingResponseCollector(node, promises)
      val pingReq = Pinger.PingRequest(UUID.randomUUID().toString, collector, node, pingTimeout)
      registeredVoters.values.foreach(_ ! pingReq)

      val evalTimeout = Duration.fromNanos((pingTimeout.duration.toNanos * PingTimeoutReceiptFudge).asInstanceOf[Long])
      val task = context.system.scheduler.scheduleOnce(evalTimeout, self, EvaluateTimeout(node, promises.mapValues(_.future)))
      pendingEval += (node -> (collector, task))

      log.info("will check on status of distributed ping request to [{}] in {}", node, evalTimeout)

    case EvaluateTimeout(node, pollResults) if (unreachable contains node) && (pendingEval contains node) =>
      forgetUnreachable(node) // we will either down it or schedule a re-eval

      // N.B. we require an affirmative PingTimeout response, rather than a timeout on the future
      val timeouts = for {
        (address, future) <- pollResults
        Success(Pinger.PingTimeout(_)) <- future.value
      } yield address

      if (timeouts.size >= votingMembers.quorumSize) {
        log.warning("downing [{}] as it was determined to be unreachable by quorum: {}", node, timeouts.mkString("[", ",", "]"))
        cluster.down(node)
      } else {
        evalAfterDelay(node, "failed to conclusively determine unreachability")
      }

  }

  private def evalAfterDelay(node: Address, reason: String) {
    context.system.scheduler.scheduleOnce(evalDelay, self, Evaluate(node))
    log.info("scheduled eval for [{}] in {} because [{}]", node, evalDelay, reason)
  }

  private def pingResponseCollector(node: Address, promises: Map[Address, Promise[Pinger.PingResponse]]) =
    context.actorOf(Props(new Actor {
      override def receive = {
        case rsp: Pinger.PingResponse =>
          val senderAddress = sender().path.address
          promises(if (senderAddress.hasGlobalScope) senderAddress else cluster.selfAddress).success(rsp)
      }
    }))

  private def forgetUnreachable(node: Address) {
    unreachable -= node
    if (pendingEval contains node) {
      log.debug("withdrawing pending eval for [{}]", node)
      reap(pendingEval(node))
      pendingEval -= node
    }
  }

  private def reap(eval: (ActorRef, Cancellable)) {
    eval match {
      case (collectorRef, task) =>
        task.cancel()
        collectorRef ! PoisonPill
    }
  }

}
