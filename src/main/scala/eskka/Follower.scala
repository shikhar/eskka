package eskka

import scala.collection.JavaConversions._
import scala.concurrent.Promise
import scala.util.{ Failure, Success }

import akka.actor.{ Actor, ActorLogging, ActorRef }
import akka.pattern.pipe

import org.elasticsearch.cluster.{ ClusterService, ClusterState }
import org.elasticsearch.cluster.metadata.MetaData
import org.elasticsearch.cluster.node.DiscoveryNode

class Follower(localNode: DiscoveryNode, clusterService: ClusterService, masterProxy: ActorRef) extends Actor with ActorLogging {

  log.info("Follower is up on node [{}]", localNode)

  import context.dispatcher

  private[this] val firstSubmit = Promise[Protocol.Transition]()

  private def submitUpdateFromMaster(updatedState: ClusterState) =
    SubmitClusterStateUpdate(log, clusterService, "eskka-follower", {
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

    case Protocol.CheckInit =>
      firstSubmit.future pipeTo sender

    case Protocol.WhoYou =>
      sender ! localNode

    case Protocol.Publish(clusterStateBytes, ackHandler) =>
      val clusterState = ClusterState.Builder.fromBytes(clusterStateBytes, localNode)

      if (clusterState.nodes.masterNodeId == localNode.id) {

        log.info("Ignoring Publish message with cluster state version [{}] as this is the master node", clusterState.version)

      } else {

        log.info("Submitting cluster state update version [{}]", clusterState.version)

        submitUpdateFromMaster(clusterState).onComplete({
          res =>
            firstSubmit.tryComplete(res)
            res match {
              case Success(transition) =>
                ackHandler ! Protocol.PublishAck(localNode, None)
              case Failure(error) =>
                ackHandler ! Protocol.PublishAck(localNode, Some(error))
            }
        })

      }

  }

}
