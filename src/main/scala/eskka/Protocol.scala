package eskka

import akka.actor.{ Address, ActorRef }

import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.node.DiscoveryNode

object Protocol {

  case object CheckInit

  case class QualifiedCheckInit(expectedRecipient: Address)

  case class Publish(clusterState: Array[Byte], ackHandler: ActorRef)

  case class PublishAck(node: DiscoveryNode, error: Option[Throwable])

  case object WhoYou

  case class Transition(source: String, currentState: ClusterState, prevState: ClusterState)

}
