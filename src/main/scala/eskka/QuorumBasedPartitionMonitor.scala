package eskka

import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor._
import akka.cluster.{ Cluster, ClusterEvent, MemberStatus }
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.duration.{ Duration, FiniteDuration }
import scala.concurrent.forkjoin.ThreadLocalRandom
import scala.concurrent.{ Future, Promise }
import scala.util.Success

object QuorumBasedPartitionMonitor {

  def props(votingMembers: VotingMembers, evalDelay: FiniteDuration, pingTimeout: Timeout) =
    Props(classOf[QuorumBasedPartitionMonitor], votingMembers, evalDelay, pingTimeout)

  private val SkipMemberStatus = Set[MemberStatus](MemberStatus.Down, MemberStatus.Exiting)

  private val EvalDelayFudge = 0.8

  private val PingTimeoutReceiptFudge = 0.25

  private case class EnrollVoter(node: Address)

  private case class VoterRegistration(node: Address, ref: ActorRef)

  private case class Evaluate(node: Address)

  private case class EvaluateTimeout(node: Address, pollResults: Map[Address, Future[Pinger.PingResponse]])

}

class QuorumBasedPartitionMonitor(votingMembers: VotingMembers, evalDelay: FiniteDuration, pingTimeout: Timeout) extends Actor with ActorLogging {

  import QuorumBasedPartitionMonitor._
  import context.dispatcher

  val cluster = Cluster(context.system)

  var franchisedVoters: Set[Address] = Set.empty
  var registeredVoters: Map[Address, ActorRef] = Map.empty

  var unreachable: Set[Address] = Set.empty
  var pendingEval: Map[Address, (ActorRef, Cancellable)] = Map.empty

  require(votingMembers.addresses(cluster.selfAddress) && cluster.selfRoles.contains(Roles.Voter))

  override def preStart() {
    cluster.subscribe(self, ClusterEvent.InitialStateAsEvents, classOf[ClusterEvent.MemberEvent], classOf[ClusterEvent.ReachabilityEvent])
  }

  override def postStop() {
    cluster.unsubscribe(self)
    pendingEval.values.foreach(reap)
  }

  override def receive = {

    case ev @ EnrollVoter(node) if franchisedVoters contains node =>
      val pinger = RootActorPath(node) / "user" / ActorNames.Pinger
      val id = UUID.randomUUID
      implicit val timeout = pingTimeout
      (context.actorSelection(pinger) ? Identify(id)).mapTo[ActorIdentity] onComplete {
        case Success(ActorIdentity(i, Some(ref))) if i == id => self ! VoterRegistration(node, ref)
        case msg =>
          log.debug("unexpected reply trying to enroll voter [{}] -- {}", node, msg)
          context.system.scheduler.scheduleOnce(evalDelay, self, ev) // retry, overloading evalDelay for this purpose...
      }

    case VoterRegistration(node, ref) if franchisedVoters contains node =>
      log.debug("registered [{}] as a voter at [{}]", node, ref)
      registeredVoters += (node -> ref)

    case mEvent: ClusterEvent.MemberEvent => mEvent match {

      case ClusterEvent.MemberWeaklyUp(m) =>
      // Ignore

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
        if (!SkipMemberStatus(m.status)) {
          unreachable += m.address
          evalAfterDelay(m.address, "unreachable")
        }

      case ClusterEvent.ReachableMember(m) =>
        forgetUnreachable(m.address)

    }

    case Evaluate(node) if (unreachable contains node) && !(pendingEval contains node) =>
      val promises = registeredVoters.mapValues(_ => Promise[Pinger.PingResponse]()).view.force
      val collector = pingResponseCollector(promises)
      val pingReq = Pinger.PingRequest(UUID.randomUUID().toString, collector, node, pingTimeout)
      registeredVoters.values.foreach(_ ! pingReq)

      val evalTimeout = Duration.fromNanos((pingTimeout.duration.toNanos * (1 + PingTimeoutReceiptFudge)).asInstanceOf[Long])
      val task = context.system.scheduler.scheduleOnce(evalTimeout, self, EvaluateTimeout(node, promises.mapValues(_.future)))
      pendingEval += (node -> (collector, task))

      log.debug("will check on status of distributed ping request to [{}] in {}", node, evalTimeout)

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

  def fudgedEvalDelay: FiniteDuration = {
    val timeoutSeconds = evalDelay.toSeconds
    val fudgeSeconds = (timeoutSeconds * EvalDelayFudge).asInstanceOf[Long]
    Duration(ThreadLocalRandom.current().nextLong(timeoutSeconds - fudgeSeconds, timeoutSeconds + fudgeSeconds), TimeUnit.SECONDS)
  }

  def evalAfterDelay(node: Address, reason: String) {
    val delay = fudgedEvalDelay
    context.system.scheduler.scheduleOnce(delay, self, Evaluate(node))
    log.debug("scheduled eval for [{}] in {} because [{}]", node, delay, reason)
  }

  def pingResponseCollector(promises: Map[Address, Promise[Pinger.PingResponse]]): ActorRef =
    context.actorOf(Props(new Actor {
      override def receive = {
        case rsp: Pinger.PingResponse =>
          val senderAddress = sender().path.address
          promises(if (senderAddress.hasGlobalScope) senderAddress else cluster.selfAddress).success(rsp)
      }
    }))

  def forgetUnreachable(node: Address) {
    unreachable -= node
    if (pendingEval contains node) {
      log.debug("withdrawing pending eval for [{}]", node)
      reap(pendingEval(node))
      pendingEval -= node
    }
  }

  def reap(eval: (ActorRef, Cancellable)) {
    eval match {
      case (collectorRef, task) =>
        task.cancel()
        collectorRef ! PoisonPill
    }
  }

}
