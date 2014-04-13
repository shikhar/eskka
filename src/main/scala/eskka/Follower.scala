package eskka

import scala.collection.JavaConversions._
import scala.util.{Failure, Success}

import akka.actor.Actor

import org.elasticsearch.cluster.{ClusterService, ClusterState}
import org.elasticsearch.cluster.metadata.MetaData
import org.elasticsearch.cluster.node.DiscoveryNode

class Follower(localNode: DiscoveryNode, clusterService: ClusterService) extends Actor {

  import scala.concurrent.ExecutionContext.Implicits.global

  private def submitUpdateFromMaster(updatedState: ClusterState) = SubmitClusterStateUpdate(clusterService, "eskka-follower", {
    currentState =>
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
  })

  override def receive = {

    case Protocol.WhoYou =>
      sender ! localNode

    case Protocol.Publish(clusterState, ackHandler) =>
      if (clusterState.nodes().masterNodeId() != localNode.id()) {

        submitUpdateFromMaster(clusterState).onComplete({
          case Success(transition) => ackHandler ! Protocol.PublishAck(localNode, None)
          case Failure(error) => ackHandler ! Protocol.PublishAck(localNode, Some(error))
        })

      }

  }

}
