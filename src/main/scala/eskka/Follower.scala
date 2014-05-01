package eskka

import java.util.concurrent.TimeUnit

import scala.Some
import scala.collection.JavaConversions._
import scala.concurrent.Promise
import scala.concurrent.duration.Duration
import scala.util.{ Failure, Success }

import akka.actor._
import akka.cluster.Cluster
import akka.pattern.pipe

import org.elasticsearch.cluster.{ ClusterService, ClusterState }
import org.elasticsearch.cluster.block.ClusterBlocks
import org.elasticsearch.cluster.metadata.MetaData
import org.elasticsearch.cluster.node.{ DiscoveryNode, DiscoveryNodes }
import org.elasticsearch.cluster.routing.RoutingTable
import org.elasticsearch.discovery.Discovery
import org.elasticsearch.gateway.GatewayService

object Follower {

  def props(localNode: DiscoveryNode, votingMembers: VotingMembers, clusterService: ClusterService, master: ActorRef) =
    Props(classOf[Follower], localNode, votingMembers, clusterService, master)

  private val QuorumCheckInterval = Duration(250, TimeUnit.MILLISECONDS)

  private case object QuorumCheck

}

class Follower(localNode: DiscoveryNode, votingMembers: VotingMembers, clusterService: ClusterService, master: ActorRef) extends Actor with ActorLogging {

  import Follower._

  import context.dispatcher

  private[this] val cluster = Cluster(context.system)

  private[this] val firstSubmit = Promise[SubmitClusterStateUpdate.Transition]()

  private[this] val quorumCheckTask = context.system.scheduler.schedule(QuorumCheckInterval, QuorumCheckInterval, self, QuorumCheck)

  private[this] var quorumCheckLastResult = true
  private[this] var pendingPublishRequest = false

  override def postStop() {
    quorumCheckTask.cancel()
  }

  override def receive = {

    case Protocol.CheckInit(expectedRecipient) if expectedRecipient == cluster.selfAddress =>
      firstSubmit.future pipeTo sender

    case Protocol.WhoYou =>
      sender ! Protocol.IAm(self, localNode)

    case Protocol.LocalMasterPublish =>
      pendingPublishRequest = false

    case Protocol.Publish(version, serializedClusterState) =>
      val updatedState = ClusterState.Builder.fromBytes(serializedClusterState, localNode)
      require(version == updatedState.version, s"Deserialized cluster state version mismatch, expected=$version, have=${updatedState.version}")
      require(updatedState.nodes.masterNodeId != localNode.id, "Master's local follower should not receive Publish messages")

      if (quorumCheckLastResult) {
        log.info("submitting publish of cluster state version {}...", version)
        SubmitClusterStateUpdate(clusterService, "follower{master-publish}", updateClusterState(updatedState)) onComplete {
          res =>
            firstSubmit.tryComplete(res)
            res match {
              case Success(transition) =>
                log.info("successfully submitted cluster state version {}", version)
                sender ! Protocol.PublishAck(localNode, None)
              case Failure(error) =>
                log.error(error, "failed to submit cluster state version {}", version)
                sender ! Protocol.PublishAck(localNode, Some(error))
            }
        }
      } else {
        log.warning("discarding publish of cluster state version {} as quorum unavailable", version)
        sender ! Protocol.PublishAck(localNode, Some(new Protocol.QuorumUnavailable))
      }

      pendingPublishRequest = false

    case QuorumCheck =>
      val currentResult = votingMembers.quorumAvailable(cluster.state)

      if (currentResult != quorumCheckLastResult) {
        if (currentResult) {
          pendingPublishRequest = true
        } else {
          SubmitClusterStateUpdate(clusterService, "follower{quorum-loss}", clearClusterState) onComplete {
            case Success(_) => log.info("quorum loss -- cleared cluster state")
            case Failure(e) => log.error(e, "quorum loss -- failed to clear cluster state")
          }
          pendingPublishRequest = false
        }
      }

      if (pendingPublishRequest) {
        log.debug("quorum available, requesting publish from master")
        master ! Protocol.PleasePublishDiscoveryState(cluster.selfAddress)
      }

      quorumCheckLastResult = currentResult

  }

  private def updateClusterState(updatedState: ClusterState)(currentState: ClusterState) = {
    val builder = ClusterState.builder(updatedState)

    // if the routing table did not change, use the original one
    if (updatedState.routingTable.version == currentState.routingTable.version) {
      builder.routingTable(currentState.routingTable)
    }

    // same for metadata
    if (updatedState.metaData.version == currentState.metaData.version) {
      builder.metaData(currentState.metaData)
    } else {
      val metaDataBuilder = MetaData.builder(updatedState.metaData).removeAllIndices()
      for (indexMetaData <- updatedState.metaData) {
        val currentIndexMetaData = currentState.metaData.index(indexMetaData.index)
        metaDataBuilder.put(
          if (currentIndexMetaData == null || currentIndexMetaData.version != indexMetaData.version)
            indexMetaData
          else
            currentIndexMetaData,
          false
        )
      }
      builder.metaData(metaDataBuilder)
    }

    builder.build
  }

  private def clearClusterState(currentState: ClusterState) =
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
