package eskka

import akka.actor.{ ActorRef, Address }

import org.elasticsearch.cluster.node.DiscoveryNode

object Protocol {

  case class CheckInit(expectedRecipient: Address)

  case class LocalMasterPublishNotification(version: Long)

  case class Publish(version: Long, serializedClusterState: Array[Byte])

  case class PleasePublishDiscoveryState(requestor: Address)

  case class PublishAck(node: DiscoveryNode, error: Option[Throwable])

  case object WhoYou

  case class IAm(ref: ActorRef, node: DiscoveryNode)

  class QuorumUnavailable extends Exception

}
