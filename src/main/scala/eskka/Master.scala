package eskka

import java.util.concurrent.TimeUnit

import scala.collection.immutable
import scala.concurrent.{ Future, Promise }
import scala.concurrent.duration.FiniteDuration
import scala.util.Success

import akka.actor._
import akka.cluster.{ Cluster, ClusterEvent, MemberStatus }
import akka.pattern.pipe
import akka.util.Timeout

import org.elasticsearch.cluster.{ ClusterService, ClusterState }
import org.elasticsearch.cluster.block.ClusterBlocks
import org.elasticsearch.cluster.metadata.MetaData
import org.elasticsearch.cluster.node.{ DiscoveryNode, DiscoveryNodes }
import org.elasticsearch.cluster.routing.RoutingTable
import org.elasticsearch.cluster.routing.allocation.AllocationService
import org.elasticsearch.discovery.Discovery
import org.elasticsearch.gateway.GatewayService

class Master(localNode: DiscoveryNode, clusterService: ClusterService, allocationService: AllocationService, publishTick: FiniteDuration)
    extends Actor with ActorLogging {

  import Master._

  import context.dispatcher

  private[this] val cluster = Cluster(context.system)

  private[this] val firstSubmit = Promise[SubmitClusterStateUpdate.Transition]()

  private[this] var discoveredNodes = Map[Address, Future[(ActorRef, DiscoveryNode)]]()
  private[this] var pendingSubmits = immutable.Queue[String]()

  private[this] val drainage = context.system.scheduler.schedule(publishTick, publishTick, self, DrainQueuedSubmits)

  override def preStart() {
    log.info("Master actor starting up on node [{}]", localNode)
    cluster.subscribe(self, ClusterEvent.InitialStateAsEvents, classOf[ClusterEvent.ReachabilityEvent], classOf[ClusterEvent.MemberEvent])
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

  private def gotQuorumOfSeedNodes = {
    val seedNodes = cluster.settings.SeedNodes
    val cs = cluster.state
    seedNodes.count(
      // only take into account Up and reachable members
      cs.members.filter(m => m.status == MemberStatus.Up && !cs.unreachable(m)).map(_.address)
    ) >= (seedNodes.size / 2) + 1
  }

  private def discoveryNodesBuilder = DiscoveryNodes.builder.put(localNode).localNodeId(localNode.id)

  private def clearClusterState(currentState: ClusterState): ClusterState = {
    ClusterState.builder(currentState)
      .blocks(
        ClusterBlocks.builder.blocks(currentState.blocks)
          .addGlobalBlock(Discovery.NO_MASTER_BLOCK)
          .addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)
          .build)
      .nodes(discoveryNodesBuilder)
      .routingTable(RoutingTable.builder)
      .metaData(MetaData.builder)
      .build
  }

  override def receive = {

    case Protocol.QualifiedCheckInit(expectedRecipient) if expectedRecipient == cluster.selfAddress =>
      firstSubmit.future pipeTo sender

    case publishMsg: Protocol.Publish =>
      val remoteFollowers = discoveredNodes.values.flatMap(_.value.flatMap {
        case Success((ref, node)) if node.id != localNode.id => Some(ref)
        case _ => None
      })
      log.info("publishing cluster state version [{}] to [{}]", publishMsg.version, remoteFollowers.mkString(","))
      remoteFollowers.foreach(_ forward publishMsg)

    case DrainQueuedSubmits =>
      if (!pendingSubmits.isEmpty) {
        val submission = SubmitClusterStateUpdate(clusterService, s"eskka-master${pendingSubmits.mkString("[", " :: ", "]")}", {
          currentState =>

            if (gotQuorumOfSeedNodes) {

              val newState = ClusterState.builder(currentState)
                .nodes(addDiscoveredNodes(discoveryNodesBuilder.masterNodeId(localNode.id)))
                .blocks(ClusterBlocks.builder.blocks(currentState.blocks).removeGlobalBlock(Discovery.NO_MASTER_BLOCK).build)
                .build

              if (newState.nodes.size < currentState.nodes.size) {
                // @see ZenDiscovery handleLeaveRequest() handleNodeFailure()
                // eagerly run reroute to remove dead nodes from routing table
                ClusterState.builder(newState).routingResult(allocationService.reroute(newState)).build
              } else {
                newState
              }

            } else {

              log.warning("Don't have quorum of seed nodes, submitting cleared state")
              clearClusterState(currentState) // @see ZenDiscovery.rejoin()

            }
        })

        firstSubmit.tryCompleteWith(submission)
        pendingSubmits = immutable.Queue()
      }

    case me: ClusterEvent.MemberEvent =>
      log.debug("member event: {}", me)
      me match {
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

    case re: ClusterEvent.ReachabilityEvent =>
      log.debug("reachability event: {}", re)
      re match {
        case ClusterEvent.ReachableMember(m) =>
          if (cluster.settings.SeedNodes.contains(m.address)) {
            // this is so that we revaluate gotQuorumOfSeedNodes, where we aggressively take unreachability into account
            self ! EnqueueSubmit(s"seed_reachable${m.address}")
          }

        case ClusterEvent.UnreachableMember(_) =>
      }

    case EnqueueSubmit(info) =>
      pendingSubmits = pendingSubmits enqueue info

  }

  override def postStop() {
    drainage.cancel()
    cluster.unsubscribe(self)
    log.info("Master actor stopped on node [{}]", localNode)
  }

}

object Master {

  private val WhoYouTimeout = Timeout(1, TimeUnit.SECONDS)

  private case object DrainQueuedSubmits

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

}
