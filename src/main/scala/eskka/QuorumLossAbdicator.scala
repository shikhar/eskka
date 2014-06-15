package eskka

import java.util.concurrent.TimeUnit

import akka.actor._
import akka.cluster.{ Cluster, MemberStatus }
import org.elasticsearch.cluster.block.ClusterBlocks
import org.elasticsearch.cluster.metadata.MetaData
import org.elasticsearch.cluster.node.{ DiscoveryNode, DiscoveryNodes }
import org.elasticsearch.cluster.routing.RoutingTable
import org.elasticsearch.cluster.{ ClusterService, ClusterState }
import org.elasticsearch.common.Priority
import org.elasticsearch.discovery.Discovery
import org.elasticsearch.gateway.GatewayService

import scala.concurrent.duration.Duration
import scala.util.{ Failure, Success }

object QuorumLossAbdicator {

  def props(localNode: DiscoveryNode, votingMembers: VotingMembers, clusterService: ClusterService, killSeq: Seq[ActorRef], restartHook: () => Unit) =
    Props(classOf[QuorumLossAbdicator], localNode, votingMembers, clusterService, killSeq, restartHook)

  private val CheckInterval = Duration(1, TimeUnit.SECONDS)

  private case object Check

  private case object Abdicate

}

class QuorumLossAbdicator(localNode: DiscoveryNode,
                          votingMembers: VotingMembers,
                          clusterService: ClusterService,
                          killSeq: Seq[ActorRef],
                          restartHook: () => Unit)
  extends Actor with ActorLogging {

  import QuorumLossAbdicator._
  import context.dispatcher

  val cluster = Cluster(context.system)

  val abdicationCheck = context.system.scheduler.schedule(CheckInterval, CheckInterval, self, Check)

  override def postStop() {
    abdicationCheck.cancel()
  }

  override def receive = {
    case Check =>
      val state = cluster.state
      val upVoters = state.members.filter(m => m.status == MemberStatus.up && votingMembers.addresses(m.address))
      val availableVoters = upVoters.filterNot(state.unreachable)
      if (availableVoters.size < votingMembers.quorumSize) {
        log.warning("abdicating -- quorum unavailable {}", (votingMembers.addresses -- availableVoters.map(_.address)).mkString("[", ",", "]"))
        abdicationCheck.cancel()
        context.become(abdicate)
        self ! Abdicate
      }
  }

  def abdicate: Actor.Receive = {
    case Abdicate =>
      killSeq.foreach(context.stop)
      SubmitClusterStateUpdate(clusterService, "eskka-quorum-loss-abdicator", Priority.URGENT, clearClusterState) onComplete {
        case Success(_) =>
          log.debug("cleared cluster state, now invoking restart hook")
          restartHook()
        case Failure(e) =>
          log.error(e, "failed to clear cluster state, will retry in {}", CheckInterval)
          context.system.scheduler.scheduleOnce(CheckInterval, self, Abdicate)
      }
  }

  def clearClusterState(currentState: ClusterState) =
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
