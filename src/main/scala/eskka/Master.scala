package eskka

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, RootActorPath}
import akka.cluster.{Cluster, Member}
import akka.cluster.ClusterEvent.{MemberEvent, MemberExited, MemberRemoved, MemberUp}
import akka.pattern.{ask, pipe}
import akka.util.Timeout

import org.elasticsearch.cluster.{ClusterService, ClusterState}
import org.elasticsearch.cluster.block.ClusterBlocks
import org.elasticsearch.cluster.node.{DiscoveryNode, DiscoveryNodes}
import org.elasticsearch.discovery.Discovery

class Master(localNode: DiscoveryNode, clusterService: ClusterService) extends Actor {

  import scala.concurrent.ExecutionContext.Implicits.global

  val cluster = Cluster(context.system)

  var discoveredNodes = Map[Member, DiscoveryNode]()

  private def submitDiscoveredNodes(source: String) = SubmitClusterStateUpdate(clusterService, s"eskka-master-$source", {
    currentState =>
      ClusterState.builder(currentState)
        .nodes(discoveryNodes)
        .blocks(ClusterBlocks.builder.blocks(currentState.blocks).removeGlobalBlock(Discovery.NO_MASTER_BLOCK).build)
        .build
  })

  private def discoveryNodes = {
    val builder = DiscoveryNodes.builder()
    for (knownNode <- discoveredNodes.values) {
      builder.put(knownNode)
    }
    builder.localNodeId(localNode.id).masterNodeId(localNode.id).put(localNode)
    builder.build()
  }

  override def receive = {

    case Protocol.Init =>
      submitDiscoveredNodes("init") pipeTo sender
      cluster.subscribe(self, classOf[MemberEvent])

    case Protocol.Publish(clusterState, ackHandler) =>
      val currentClusterState = ClusterState.builder(clusterState).nodes(discoveryNodes).build()
      context.actorSelection(cluster.system / "user" / "eskka-follower") ! Protocol.Publish(currentClusterState, ackHandler)

    case me: MemberEvent => me match {
      case MemberUp(m) =>
        val timeout = Timeout(5, TimeUnit.SECONDS)
        (context.actorSelection(RootActorPath(m.address) / "user" / "follower") ? Protocol.WhoYou)(timeout).onSuccess({
          case node: DiscoveryNode =>
            discoveredNodes += (m -> node)
            submitDiscoveredNodes("memberUp")
        })
      case MemberExited(m) =>
        discoveredNodes -= m
        submitDiscoveredNodes("memberExited")
      case MemberRemoved(m, prevStatus) =>
        discoveredNodes -= m
        submitDiscoveredNodes("memberRemoved")
    }

  }

}