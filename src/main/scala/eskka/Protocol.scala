package eskka

import akka.actor.{ ActorRef, Address }

import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.node.DiscoveryNode
import scala.util.Try

object Protocol {

  case object CheckInit

  case class ClusterStateTransition(source: String, currentState: ClusterState, prevState: ClusterState)

  case class LocalMasterPublishNotification(transition: Try[ClusterStateTransition])

  case class MasterPublish(clusterState: ClusterState)

  case class FollowerPublish(version: Long, serializedClusterState: Array[Byte])

  case class PleasePublishDiscoveryState(requestor: Address)

  case class PublishAck(node: DiscoveryNode, error: Option[Throwable])

  case object WhoYou

  case class IAm(ref: ActorRef, node: DiscoveryNode)

  class QuorumUnavailable extends Exception

}
