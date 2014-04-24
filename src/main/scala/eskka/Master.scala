package eskka

import java.util.concurrent.TimeUnit

import scala.collection.immutable
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.FiniteDuration

import akka.actor._
import akka.cluster.{Cluster, ClusterEvent, Member, MemberStatus}
import akka.pattern.{ask, pipe}
import akka.util.Timeout

import org.elasticsearch.cluster.{ClusterService, ClusterState}
import org.elasticsearch.cluster.block.ClusterBlocks
import org.elasticsearch.cluster.metadata.MetaData
import org.elasticsearch.cluster.node.{DiscoveryNode, DiscoveryNodes}
import org.elasticsearch.cluster.routing.RoutingTable
import org.elasticsearch.cluster.routing.allocation.AllocationService
import org.elasticsearch.discovery.Discovery
import org.elasticsearch.gateway.GatewayService

class Master(localNode: DiscoveryNode, clusterService: ClusterService, allocationService: AllocationService, publishTick: FiniteDuration)
    extends Actor with ActorLogging {

  import Master._

  import context.dispatcher

  private[this] val cluster = Cluster(context.system)

  private[this] val firstSubmit = Promise[Protocol.Transition]()

  private[this] var discoveredNodes = Map[Address, Future[DiscoveryNode]]()
  private[this] var pendingSubmits = immutable.Queue[String]()

  private[this] val drainage = context.system.scheduler.schedule(publishTick, publishTick, self, DrainQueuedSubmits)

  override def preStart() {
    log.info("Master actor starting up on node [{}]", localNode)
    cluster.subscribe(self, ClusterEvent.InitialStateAsEvents, classOf[ClusterEvent.MemberEvent])
  }

  private def addDiscoveredNodes(builder: DiscoveryNodes.Builder) = {
    for {
      nodeFuture <- discoveredNodes.values
      nodeValueTry <- nodeFuture.value
      node <- nodeValueTry
    } {
      builder.put(node)
    }
    builder
  }

  override def receive = {

    case Protocol.QualifiedCheckInit(expectedRecipient) if expectedRecipient == cluster.selfAddress =>
      firstSubmit.future pipeTo sender

    case Protocol.Publish(clusterState, ackHandler) =>
      val allButMe = discoveredNodes.keys.filter(_ != cluster.selfAddress).toSeq
      if (!allButMe.isEmpty) {
        log.info("publishing to [{}]", allButMe.mkString(","))
        ackHandler ! allButMe.size
        for (address <- allButMe) {
          context.actorSelection(RootActorPath(address) / "user" / ActorNames.Follower) ! Protocol.Publish(clusterState, ackHandler)
        }
      }

    case DrainQueuedSubmits =>
      if (!pendingSubmits.isEmpty) {
        val submission = SubmitClusterStateUpdate(log, clusterService, s"eskka-master${pendingSubmits.mkString("[", " :: ", "]")}", {
          currentState =>

            if (gotQuorumOfSeedNodes) {

              val newState = ClusterState.builder(currentState)
                .nodes(addDiscoveredNodes(DiscoveryNodes.builder.put(localNode).localNodeId(localNode.id).masterNodeId(localNode.id)))
                .blocks(ClusterBlocks.builder.blocks(currentState.blocks).removeGlobalBlock(Discovery.NO_MASTER_BLOCK).build)
                .build

              if (newState.nodes.size < currentState.nodes.size) {
                // eagerly run reroute to remove dead nodes from routing table
                ClusterState.builder(newState).routingResult(allocationService.reroute(newState)).build
              } else {
                newState
              }

            } else {

              log.warning("Don't have quorum of seed nodes, submitting init state")

              ClusterState.builder(currentState)
                .blocks(
                  ClusterBlocks.builder.blocks(currentState.blocks)
                    .addGlobalBlock(Discovery.NO_MASTER_BLOCK)
                    .addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)
                    .build)
                .nodes(DiscoveryNodes.builder.put(localNode).localNodeId(localNode.id))
                .routingTable(RoutingTable.builder)
                .metaData(MetaData.builder)
                .build

            }
        })

        firstSubmit.tryCompleteWith(submission)
        pendingSubmits = immutable.Queue()
      }

    case me: ClusterEvent.MemberEvent =>
      log.debug("member event: {}", me)
      me match {
        case ClusterEvent.MemberUp(m) =>
          implicit val timeout = WhoYouTimeout
          discoveredNodes += (m.address -> whoYou(m)(self ! EnqueueSubmit(s"up(${m.address})")))
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

    case EnqueueSubmit(info) =>
      pendingSubmits = pendingSubmits enqueue info

  }

  private def whoYou(m: Member)(postComplete: => Unit)(implicit timeout: Timeout) = {
    val p = Promise[DiscoveryNode]()
    (context.actorSelection(RootActorPath(m.address) / "user" / ActorNames.Follower) ? Protocol.WhoYou).mapTo[DiscoveryNode].onComplete {
      x =>
        p.complete(x)
        postComplete
    }
    p.future
  }

  override def postStop() {
    drainage.cancel()
    cluster.unsubscribe(self)
    log.info("Master actor stopped on node [{}]", localNode)
  }

  private def gotQuorumOfSeedNodes = {
    val seedNodes = cluster.settings.SeedNodes
    val cs = cluster.state
    seedNodes.count(
      cs.members.filter(m => m.status == MemberStatus.Up && !cs.unreachable(m)).map(_.address)
    ) >= (seedNodes.size / 2) + 1
  }

}

object Master {

  private val WhoYouTimeout = Timeout(5, TimeUnit.SECONDS)

  private case object DrainQueuedSubmits

  private case class EnqueueSubmit(info: String)

}
