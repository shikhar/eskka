package eskka

import akka.actor._
import akka.util.Timeout
import org.elasticsearch.cluster.node.DiscoveryNode
import org.elasticsearch.discovery.Discovery.AckListener
import org.elasticsearch.threadpool.ThreadPool

object PublishResponseHandler {

  def props(acksExpected: Set[DiscoveryNode], threadPool: ThreadPool, ackListener: AckListener, timeout: Timeout) =
    Props(classOf[PublishResponseHandler], acksExpected, threadPool, ackListener, timeout)

  case object FullyAcked

}

class PublishResponseHandler(acksExpected: Set[DiscoveryNode], threadPool: ThreadPool, ackListener: AckListener, timeout: Timeout)
  extends Actor with ActorLogging {

  import context.dispatcher

  context.system.scheduler.scheduleOnce(timeout.duration, self, PoisonPill)

  var pendingNodes = acksExpected

  override def receive = {
    case PublishAck(node, error) =>
      val deserDiscoveryNode = DiscoveryNodeSerialization.fromBytes(node)
      threadPool.generic().execute(new Runnable {
        override def run() {
          ackListener.onNodeAck(deserDiscoveryNode, error.orNull)
        }
      })
      pendingNodes -= deserDiscoveryNode
      if (pendingNodes.isEmpty) {
        context.stop(self)
      }
  }

}

