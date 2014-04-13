package eskka

import akka.actor.ActorRef

import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.node.DiscoveryNode

object Protocol {

  case object Init

  case class Publish(clusterState: ClusterState, ackHandler: ActorRef)

  case class PublishAck(node: DiscoveryNode, error: Option[Throwable])

  case object WhoYou

}
