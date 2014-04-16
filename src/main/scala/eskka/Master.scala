package eskka

import java.util.concurrent.TimeUnit
import scala.concurrent.Promise

import akka.actor.{ Actor, ActorLogging, RootActorPath }
import akka.cluster.{ Cluster, ClusterEvent, Member }
import akka.pattern.{ ask, pipe }
import akka.util.Timeout

import org.elasticsearch.cluster.{ ClusterService, ClusterState }
import org.elasticsearch.cluster.block.ClusterBlocks
import org.elasticsearch.cluster.node.{ DiscoveryNode, DiscoveryNodes }
import org.elasticsearch.discovery.Discovery
import akka.cluster.ClusterEvent.CurrentClusterState

class Master(localNode: DiscoveryNode, clusterService: ClusterService) extends Actor with ActorLogging {

  import scala.concurrent.ExecutionContext.Implicits.global

  private[this] val cluster = Cluster(context.system)
  private[this] var discoveredNodes = Map[Member, DiscoveryNode]()
  private[this] val firstSubmit = Promise[Protocol.Transition]()

  log.info("Master actor up on node [{}]", localNode)

  cluster.registerOnMemberUp({
    cluster.subscribe(self, classOf[ClusterEvent.MemberEvent])
    submitDiscoveredNodes("init", firstSubmit)
  })

  private def submitDiscoveredNodes(source: String, promise: Promise[Protocol.Transition] = Promise()) = {
    SubmitClusterStateUpdate(log, clusterService, s"eskka-master-$source", {
      currentState =>
        ClusterState.builder(currentState)
          .nodes(discoveryNodes)
          .blocks(ClusterBlocks.builder.blocks(currentState.blocks).removeGlobalBlock(Discovery.NO_MASTER_BLOCK).build)
          .build
    }, promise)
  }

  private def discoveryNodes = {
    val builder = DiscoveryNodes.builder()
    for (knownNode <- discoveredNodes.values) {
      builder.put(knownNode)
    }
    builder.localNodeId(localNode.id).masterNodeId(localNode.id).put(localNode)
    builder.build()
  }

  override def receive = {

    // es

    case Protocol.QualifiedCheckInit(expectedRecipient) =>
      if (expectedRecipient == localNode.id) {
        firstSubmit.future pipeTo sender
      }

    case Protocol.Publish(clusterState, ackHandler) =>
      log.info("publishing to {}", discoveredNodes)
      for (m <- discoveredNodes.keys) {
        context.actorSelection(RootActorPath(m.address) / "user" / "eskka-follower") ! Protocol.Publish(clusterState, ackHandler)
      }

    // akka

    case cs: CurrentClusterState => // initial snapshot
      for (m <- cs.members) {
        whoYou(m).onSuccess({
          case node: DiscoveryNode =>
            discoveredNodes += (m -> node)
        })
      }

    case me: ClusterEvent.MemberEvent => me match {
      case ClusterEvent.MemberUp(m) =>
        log.info("memberUp: {}", m)
        whoYou(m).onSuccess({
          case node: DiscoveryNode =>
            discoveredNodes += (m -> node)
            submitDiscoveredNodes("memberUp")
        })
      case ClusterEvent.MemberExited(m) =>
        log.info("memberExited: {}", m)
        discoveredNodes -= m
        submitDiscoveredNodes("memberExited")
      case ClusterEvent.MemberRemoved(m, prevStatus) =>
        log.info("memberRemoved: {}", m)
        discoveredNodes -= m
        submitDiscoveredNodes("memberRemoved")
    }

  }

  private def whoYou(m: Member) = {
    (context.actorSelection(RootActorPath(m.address) / "user" / "eskka-follower") ? Protocol.WhoYou)(Timeout(5, TimeUnit.SECONDS))
  }

}