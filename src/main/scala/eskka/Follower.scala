package eskka

import scala.collection.JavaConversions._
import scala.concurrent.Promise
import scala.util.{ Failure, Success }

import akka.actor.{ Actor, ActorLogging, Props }
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.ClusterDomainEvent
import akka.pattern.pipe

import org.elasticsearch.cluster.{ ClusterService, ClusterState }
import org.elasticsearch.cluster.block.ClusterBlocks
import org.elasticsearch.cluster.metadata.MetaData
import org.elasticsearch.cluster.node.{ DiscoveryNode, DiscoveryNodes }
import org.elasticsearch.cluster.routing.RoutingTable
import org.elasticsearch.discovery.Discovery
import org.elasticsearch.gateway.GatewayService

object Follower {

  def props(localNode: DiscoveryNode, votingMembers: VotingMembers, clusterService: ClusterService) =
    Props(classOf[Follower], localNode, votingMembers, clusterService)

}

class Follower(localNode: DiscoveryNode, votingMembers: VotingMembers, clusterService: ClusterService)
    extends Actor with ActorLogging {

  log.info("Follower is up on node [{}]", localNode)

  import context.dispatcher

  private[this] val cluster = Cluster(context.system)

  private[this] val firstSubmit = Promise[SubmitClusterStateUpdate.Transition]()

  private[this] var quorumAvailability: Option[QuorumAvailability] = None

  override def preStart() {
    cluster.subscribe(self, classOf[ClusterDomainEvent])
  }

  override def postStop() {
    cluster.unsubscribe(self)
  }

  override def receive = {

    case Protocol.CheckInit(expectedRecipient) if expectedRecipient == cluster.selfAddress =>
      firstSubmit.future pipeTo sender

    case Protocol.WhoYou =>
      sender ! localNode

    case Protocol.Publish(version, serializedClusterState) if quorumAvailability.contains(QuorumAvailable) =>
      val updatedState = ClusterState.Builder.fromBytes(serializedClusterState, localNode)
      require(version == updatedState.version, s"Deserialized cluster state version mismatch, expected=$version, have=${updatedState.version}")
      require(updatedState.nodes.masterNodeId != localNode.id, "Master's local follower should not receive Publish messages")

      log.info("submitting updated cluster state version [{}]", version)
      SubmitClusterStateUpdate(clusterService, "follower{master-publish}", updateClusterState(updatedState)) onComplete {
        res =>
          firstSubmit.tryComplete(res)
          res match {
            case Success(transition) =>
              sender ! Protocol.PublishAck(localNode, None)
            case Failure(error) =>
              sender ! Protocol.PublishAck(localNode, Some(error))
          }
      }

    case e: ClusterDomainEvent =>
      val prevQuorumAvailability = quorumAvailability
      quorumAvailability = Some(votingMembers.quorumAvailability(cluster.state))
      if (prevQuorumAvailability != quorumAvailability && quorumAvailability.contains(QuorumUnavailable)) {
        log.warning("submitting cleared cluster state due to loss of quorum ({})", e)
        SubmitClusterStateUpdate(clusterService, "follower{quorum-loss}", clearClusterState)
      }

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
