package eskka

import java.util.concurrent.TimeUnit

import scala.collection.immutable
import scala.concurrent.Promise
import scala.concurrent.duration.Duration

import akka.actor.{ Actor, ActorLogging, Address, RootActorPath }
import akka.cluster.{ Cluster, ClusterEvent, Member }
import akka.pattern.{ ask, pipe }
import akka.util.Timeout

import org.elasticsearch.cluster.{ ClusterService, ClusterState }
import org.elasticsearch.cluster.block.ClusterBlocks
import org.elasticsearch.cluster.node.{ DiscoveryNode, DiscoveryNodes }
import org.elasticsearch.discovery.Discovery

class Master(localNode: DiscoveryNode, clusterService: ClusterService) extends Actor with ActorLogging {

  import Master._

  import context.dispatcher

  private[this] val cluster = Cluster(context.system)

  private[this] val firstSubmit = Promise[Protocol.Transition]()

  private[this] var discoveredNodes = Map[Address, DiscoveryNode]()
  private[this] var pendingSubmits = immutable.Queue[String]()

  private[this] val drainage = context.system.scheduler.schedule(submissionDrainInterval, submissionDrainInterval, self, DrainQueuedSubmits)

  override def preStart() {
    log.info("Master actor starting up on node [{}]", localNode)
    cluster.subscribe(self, ClusterEvent.InitialStateAsEvents, classOf[ClusterEvent.MemberEvent])
  }

  private def discoveryNodes = {
    val builder = DiscoveryNodes.builder()
    for (knownNode <- discoveredNodes.values) {
      builder.put(knownNode)
    }
    builder.localNodeId(localNode.id).masterNodeId(localNode.id).put(localNode)
    builder.build()
  }

  override def receive = {

    case Protocol.QualifiedCheckInit(expectedRecipient) if expectedRecipient == localNode.id =>
      firstSubmit.future pipeTo sender

    case Protocol.Publish(clusterState, ackHandler) =>
      log.info("publishing to {}", discoveredNodes)
      val allButMe = discoveredNodes.keys.filter(_ != cluster.selfAddress).toSeq
      ackHandler ! allButMe.size
      for (address <- allButMe) {
        context.actorSelection(RootActorPath(address) / "user" / "eskka-follower") ! Protocol.Publish(clusterState, ackHandler)
      }

    case DrainQueuedSubmits =>
      if (!pendingSubmits.isEmpty) {
        log.info("pending submits are [{}]", pendingSubmits)
        val submission = SubmitClusterStateUpdate(log, clusterService, s"eskka-master{${pendingSubmits.mkString("::")}}", {
          currentState =>
            ClusterState.builder(currentState)
              .nodes(discoveryNodes)
              .blocks(ClusterBlocks.builder.blocks(currentState.blocks).removeGlobalBlock(Discovery.NO_MASTER_BLOCK).build)
              .build
        })
        if (!firstSubmit.isCompleted) {
          submission.onComplete(firstSubmit.tryComplete)
        }
        pendingSubmits = immutable.Queue()
      }

    case me: ClusterEvent.MemberEvent =>
      log.debug("member event: {}", me)
      me match {
        case ClusterEvent.MemberUp(m) =>
          whoYou(m).onSuccess({
            case node: DiscoveryNode =>
              self ! AddNode(m.address, node)
          })
        case ClusterEvent.MemberExited(m) =>
          self ! RemoveNode(m.address)
        case ClusterEvent.MemberRemoved(m, prevStatus) =>
          self ! RemoveNode(m.address)
      }

    case AddNode(address, node) =>
      discoveredNodes += (address -> node)
      pendingSubmits = pendingSubmits enqueue s"add(${node.id})"

    case RemoveNode(address) =>
      for (node <- discoveredNodes.get(address)) {
        pendingSubmits = pendingSubmits enqueue s"remove(${node.id})"
        discoveredNodes -= address
      }
  }

  private def whoYou(m: Member) = {
    implicit val whoYouTimeout = Timeout(5, TimeUnit.SECONDS)
    context.actorSelection(RootActorPath(m.address) / "user" / "eskka-follower") ? Protocol.WhoYou
  }

  override def postStop() {
    drainage.cancel()
    cluster.unsubscribe(self)
    log.info("Master actor stopped on node [{}]", localNode)
  }

}

object Master {

  private case object DrainQueuedSubmits

  private case class AddNode(address: Address, node: DiscoveryNode)

  private case class RemoveNode(address: Address)

  val submissionDrainInterval = Duration(250, TimeUnit.MILLISECONDS)

}
