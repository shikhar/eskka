package eskka

import akka.actor._
import akka.util.Timeout
import org.elasticsearch.cluster.node.DiscoveryNode
import org.elasticsearch.discovery.Discovery.AckListener

object PublishResponseHandler {

  def props(acksExpected: Int, ackListener: AckListener, timeout: Timeout) = Props(classOf[PublishResponseHandler], acksExpected, ackListener, timeout)

  case object FullyAcked

}

class PublishResponseHandler(acksExpected: Set[DiscoveryNode], ackListener: AckListener, timeout: Timeout) extends Actor with ActorLogging {

  import context.dispatcher

  context.system.scheduler.scheduleOnce(timeout.duration, self, PoisonPill)

  var pendingNodes = acksExpected

  override def receive = {
    case PublishAck(node, error) =>
      ackListener.onNodeAck(node, error.orNull)
      pendingNodes -= node
      if (pendingNodes.isEmpty) {
        context.stop(self)
      }
  }

}

