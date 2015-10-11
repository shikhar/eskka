package eskka

import java.util.concurrent.TimeUnit

import akka.actor._
import akka.cluster.{ Cluster, MemberStatus }
import org.elasticsearch.cluster.block.ClusterBlocks
import org.elasticsearch.cluster.node.{ DiscoveryNode, DiscoveryNodes }
import org.elasticsearch.cluster.{ ClusterService, ClusterState }
import org.elasticsearch.common.Priority
import org.elasticsearch.discovery.DiscoverySettings

import scala.concurrent.duration.Duration
import scala.util.{ Failure, Success }

object QuorumLossAbdicator {

  private val CheckInterval = Duration(1, TimeUnit.SECONDS)

  def props(localNode: DiscoveryNode,
            votingMembers: VotingMembers,
            discoverySettings: DiscoverySettings,
            clusterService: ClusterService,
            killSeq: List[ActorRef],
            restartHook: () => Unit) =
    Props(classOf[QuorumLossAbdicator], localNode, votingMembers, discoverySettings, clusterService, killSeq, restartHook)

  private case object Check

  private case object Abdicate

}

class QuorumLossAbdicator(localNode: DiscoveryNode,
                          votingMembers: VotingMembers,
                          discoverySettings: DiscoverySettings,
                          clusterService: ClusterService,
                          killSeq: List[ActorRef],
                          restartHook: () => Unit)
  extends Actor with ActorLogging {

  import context.dispatcher
  import eskka.QuorumLossAbdicator._

  val cluster = Cluster(context.system)

  val scheduler = context.system.scheduler

  val abdicationCheck = scheduler.schedule(CheckInterval, CheckInterval, self, Check)

  override def postStop() {
    abdicationCheck.cancel()
  }

  override def receive: Actor.Receive = {
    case Check =>
      val akkaClusterState = cluster.state
      val upVoters = akkaClusterState.members.filter(m => m.status == MemberStatus.up && votingMembers.addresses(m.address))
      val availableVoters = upVoters.filterNot(akkaClusterState.unreachable)
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

      SubmitClusterStateUpdate(clusterService, "eskka-quorum-loss-abdicator", Priority.URGENT, runOnlyOnMaster = false, abdicationClusterState) onComplete {
        case Success(_) =>
          log.debug("cleared cluster state, now invoking restart hook")
          restartHook()
        case Failure(e) =>
          log.error(e, "failed to clear cluster state, will retry in {}", CheckInterval)
          scheduler.scheduleOnce(CheckInterval, self, Abdicate)
      }
  }

  def abdicationClusterState(currentState: ClusterState): ClusterState =
    ClusterState.builder(currentState)
      .blocks(
        ClusterBlocks.builder.blocks(currentState.blocks)
          .addGlobalBlock(discoverySettings.getNoMasterBlock)
          .build)
      .nodes(DiscoveryNodes.builder.put(localNode).localNodeId(localNode.id))
      .build

}
