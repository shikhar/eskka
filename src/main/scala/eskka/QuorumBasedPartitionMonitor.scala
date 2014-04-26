package eskka

import scala.Some
import scala.concurrent.Future
import scala.util.Success

import akka.actor._
import akka.cluster.{ Cluster, ClusterEvent }
import akka.pattern.{ ask, pipe }
import akka.util.Timeout

object QuorumBasedPartitionMonitor {

  def props(votingMembers: VotingMembers, replyTimeout: Timeout) = Props(classOf[QuorumBasedPartitionMonitor], votingMembers, replyTimeout)

  private case class PingReq(node: Address)

  private case class PingRsp(node: Address)

  private case class Evaluate(node: Address, pingResults: Seq[Future[PingRsp]])

}

class QuorumBasedPartitionMonitor(votingMembers: VotingMembers, replyTimeout: Timeout) extends Actor with ActorLogging {

  import QuorumBasedPartitionMonitor._

  import context.dispatcher

  val cluster = Cluster(context.system)

  require(votingMembers.addresses(cluster.selfAddress))

  var voterRefs: Map[Address, ActorRef] = Map.empty

  var awaitingEval: Map[Address, Cancellable] = Map.empty

  override def preStart() {
    cluster.subscribe(self, ClusterEvent.InitialStateAsEvents, classOf[ClusterEvent.MemberEvent], classOf[ClusterEvent.ReachableMember])
  }

  override def postStop() {
    cluster.unsubscribe(self)
    awaitingEval.values.foreach(_.cancel())
  }

  override def receive = {

    // we may now know the ActorRef of our correspondent on another voting member
    case ActorIdentity(_, Some(ref)) if votingMembers.addresses(sender().path.address) =>
      voterRefs += (sender().path.address -> ref)

    case me: ClusterEvent.MemberEvent => me match {
      case ClusterEvent.MemberUp(m) =>
        if (votingMembers.addresses(m.address)) {
          val correspondent = RootActorPath(m.address) / context.self.path.elements.drop(1).mkString("/")
          context.actorSelection(correspondent) ! Identify
        }

      case ClusterEvent.MemberExited(m) => remove(m.address)
      case ClusterEvent.MemberRemoved(m, _) => remove(m.address)
    }

    case re: ClusterEvent.ReachabilityEvent => re match {
      case ClusterEvent.UnreachableMember(m) => scheduleEval(m.address)
      case ClusterEvent.ReachableMember(m) => remove(m.address)
    }

    // a voting member (maybe myself) has requested to check on some node
    case PingReq(node) =>
      // doesn't really matter which actor, but follower is a good bet since it's an app-level actor that should be running on all cluster members
      val recipient = context.actorSelection(RootActorPath(node) / "user" / ActorNames.Follower)
      implicit val timeout = replyTimeout
      (recipient ? Identify) collect {
        case ActorIdentity(_, Some(ref)) => PingRsp(node)
      } pipeTo sender()

    // time to check on the results of our distributed ping
    case Evaluate(node, pingResults) =>
      if (awaitingEval contains node) {

        val unreachable = pingResults.map(_.value).count({
          case Some(Success(PingRsp(n))) =>
            require(node == n); true
          case _ => false
        }) < votingMembers.quorumSize

        if (unreachable) {
          log.warning("downing [{}] as it was determined to be unreachable by quorum")
          cluster.down(node)
          remove(node)
        } else {
          scheduleEval(node)
        }

      }

  }

  private def pollVoters(node: Address) = {
    implicit val timeout = replyTimeout
    voterRefs.values.map(ref => (ref ? PingReq(node)).mapTo[PingRsp])
  }

  private def scheduleEval(node: Address) {
    val pingResults = pollVoters(node).toSeq
    val task = context.system.scheduler.scheduleOnce(replyTimeout.duration, self, Evaluate(node, pingResults))
    awaitingEval += (node -> task)
  }

  private def remove(node: Address) {
    if (awaitingEval contains node) {
      awaitingEval(node).cancel()
      awaitingEval -= node
    }
  }

}
