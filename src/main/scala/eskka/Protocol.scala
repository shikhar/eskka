package eskka

import akka.actor.Address

import org.elasticsearch.cluster.node.DiscoveryNode

object Protocol {

  case class CheckInit(expectedRecipient: Address)

  case class Publish(version: Long, serializedClusterState: Array[Byte])

  case class PublishAck(node: DiscoveryNode, error: Option[Throwable])

  case object WhoYou

}
