package eskka

import akka.actor._
import akka.util.Timeout
import org.elasticsearch.discovery.Discovery.AckListener

object PublishResponseHandler {

  def props(acksExpected: Int, ackListener: AckListener, timeout: Timeout) = Props(classOf[PublishResponseHandler], acksExpected, ackListener, timeout)

  case object SubscribeFullyAcked

  case object FullyAcked

}

class PublishResponseHandler(acksExpected: Int, ackListener: AckListener, timeout: Timeout) extends Actor with ActorLogging {

  require(acksExpected > 0)

  import PublishResponseHandler._
  import context.dispatcher

  context.system.scheduler.scheduleOnce(timeout.duration, self, PoisonPill)

  var acksReceived = 0
  var subscribers = List[ActorRef]()

  override def receive = {

    case PublishAck(node, error) =>
      ackListener.onNodeAck(node, error.orNull)
      acksReceived += 1
      if (acksReceived == acksExpected) {
        subscribers.foreach(_ ! FullyAcked)
        context.stop(self)
      }

    case SubscribeFullyAcked =>
      if (acksReceived == acksExpected) {
        context.stop(self)
        sender() ! FullyAcked
      } else {
        subscribers ::= sender()
      }

  }

}

