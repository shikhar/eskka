package eskka

import java.util.concurrent.TimeUnit

import scala.collection.immutable
import scala.concurrent.{ Future, Promise }
import scala.concurrent.duration.FiniteDuration
import scala.util.Success

import akka.actor._
import akka.cluster.{ Cluster, ClusterEvent }
import akka.pattern.pipe
import akka.util.Timeout

import org.elasticsearch.ElasticsearchException
import org.elasticsearch.cluster.{ ClusterService, ClusterState }
import org.elasticsearch.cluster.block.ClusterBlocks
import org.elasticsearch.cluster.node.{ DiscoveryNode, DiscoveryNodes }
import org.elasticsearch.cluster.routing.allocation.AllocationService
import org.elasticsearch.discovery.Discovery

object Master {

  private val WhoYouTimeout = Timeout(1, TimeUnit.SECONDS)

  private case object DrainQueuedDiscoveryChanges

  private case class EnqueueSubmit(info: String)

  private class WhoYouResponseHandler(promise: Promise[(ActorRef, DiscoveryNode)], timeout: Timeout) extends Actor {

    import context.dispatcher

    context.system.scheduler.scheduleOnce(timeout.duration, self, PoisonPill)

    override def receive = {
      case node: DiscoveryNode =>
        promise.success((sender(), node))
        context.stop(self)
    }
  }

  def props(localNode: DiscoveryNode, votingMembers: VotingMembers,
    clusterService: ClusterService, allocationService: AllocationService,
    drainageInterval: FiniteDuration) =
    Props(classOf[Master], localNode, votingMembers, clusterService, allocationService, drainageInterval)

}

class Master(localNode: DiscoveryNode, votingMembers: VotingMembers,
  clusterService: ClusterService, allocationService: AllocationService,
  drainageInterval: FiniteDuration)
    extends Actor with ActorLogging {

  import Master._

  import context.dispatcher

  private[this] val cluster = Cluster(context.system)

  private[this] val drainage = context.system.scheduler.schedule(drainageInterval, drainageInterval, self, DrainQueuedDiscoveryChanges)

  private[this] val firstSubmit = Promise[SubmitClusterStateUpdate.Transition]()

  private[this] var discoveredNodes = Map[Address, Future[(ActorRef, DiscoveryNode)]]()

  private[this] var pendingDiscoveryChanges = immutable.Queue[String]()

  override def preStart() {
    log.info("Master actor starting up on node [{}]", localNode)
    cluster.subscribe(self, ClusterEvent.InitialStateAsEvents, classOf[ClusterEvent.MemberEvent], classOf[ClusterEvent.ReachabilityEvent])
  }

  override def postStop() {
    cluster.unsubscribe(self)
    drainage.cancel()
    log.info("Master actor stopped on node [{}]", localNode)
  }

  override def receive = {

    case Protocol.CheckInit(expectedRecipient) if expectedRecipient == cluster.selfAddress =>
      firstSubmit.future pipeTo sender

    case publishMsg: Protocol.Publish =>
      if (votingMembers.quorumAvailability(cluster.state) == QuorumAvailable) {
        val remoteFollowers = discoveredNodes.values.flatMap(_.value.collect {
          case Success((ref, node)) if node.id != localNode.id => ref
        })
        log.info("publishing cluster state version [{}] to [{}]", publishMsg.version, remoteFollowers.mkString(","))
        remoteFollowers.foreach(_ forward publishMsg)
      } else {
        log.warning("don't have quorum so won't forward publish message for cluster state version [{}]", publishMsg.version)
        sender ! Protocol.PublishAck(localNode, throw new ElasticsearchException("Don't have quorum"))
      }

    case DrainQueuedDiscoveryChanges =>
      if (!pendingDiscoveryChanges.isEmpty) {
        val summary = pendingDiscoveryChanges.mkString("[", " :: ", "]")
        if (votingMembers.quorumAvailability(cluster.state) == QuorumAvailable) {
          val submission = SubmitClusterStateUpdate(clusterService, s"eskka-master$summary", discoveryState)
          firstSubmit.tryCompleteWith(submission)
        } else {
          log.warning("don't have quorum to submit pending discovery changes {}", summary)
        }
        pendingDiscoveryChanges = immutable.Queue()
      }

    case me: ClusterEvent.MemberEvent => me match {

      case ClusterEvent.MemberUp(m) =>
        val p = Promise[(ActorRef, DiscoveryNode)]()
        val whoYouResponseHandler = context.actorOf(Props(classOf[WhoYouResponseHandler], p, WhoYouTimeout))
        context.actorSelection(RootActorPath(m.address) / "user" / ActorNames.Follower).tell(Protocol.WhoYou, whoYouResponseHandler)
        discoveredNodes += (m.address -> p.future)
        p.future onSuccess {
          case _ => self ! EnqueueSubmit(s"up(${m.address})")
        }

      case ClusterEvent.MemberExited(m) =>
        if (discoveredNodes.contains(m.address)) {
          discoveredNodes -= m.address
          self ! EnqueueSubmit(s"exited(${m.address})")
        }

      case ClusterEvent.MemberRemoved(m, _) =>
        if (discoveredNodes.contains(m.address)) {
          discoveredNodes -= m.address
          self ! EnqueueSubmit(s"removed(${m.address})")
        }

    }

    case re: ClusterEvent.ReachabilityEvent => re match {

      case ClusterEvent.UnreachableMember(m) =>

      case ClusterEvent.ReachableMember(m) =>
        // a) quorum checks takes reachability into account -- we may have regained quorum upon a member becoming reachable
        // b) m's follower actor could probably do with receiving a fresh publish
        self ! EnqueueSubmit(s"reachable(${m.address})")

    }

    case EnqueueSubmit(info) =>
      pendingDiscoveryChanges = pendingDiscoveryChanges enqueue info

  }

  private def discoveryState(currentState: ClusterState): ClusterState = {
    val newState = ClusterState.builder(currentState)
      .nodes(addDiscoveredNodes(DiscoveryNodes.builder.put(localNode).localNodeId(localNode.id).masterNodeId(localNode.id)))
      .blocks(ClusterBlocks.builder.blocks(currentState.blocks).removeGlobalBlock(Discovery.NO_MASTER_BLOCK).build)
      .build

    if (newState.nodes.size < currentState.nodes.size) {
      // @see ZenDiscovery handleLeaveRequest() handleNodeFailure()
      // eagerly run reroute to remove dead nodes from routing table
      ClusterState.builder(newState).routingResult(allocationService.reroute(newState)).build
    } else {
      newState
    }
  }

  private def addDiscoveredNodes(builder: DiscoveryNodes.Builder) = {
    for {
      iAmFuture <- discoveredNodes.values
      iAmValueTry <- iAmFuture.value
      (ref, node) <- iAmValueTry
    } {
      builder.put(node)
    }
    builder
  }

}
