package eskka

import akka.actor.Address

import org.elasticsearch.cluster.node.DiscoveryNode

object Protocol {

  case object CheckInit

  case class QualifiedCheckInit(expectedRecipient: Address)

  case class Publish(version: Long, serializedClusterState: Array[Byte])

  case class PublishAck(node: DiscoveryNode, error: Option[Throwable])

  case object WhoYou

}
