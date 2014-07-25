package eskka

import akka.actor._
import akka.cluster.Cluster
import akka.pattern.pipe
import akka.util.ByteString
import org.elasticsearch.cluster.node.DiscoveryNode
import org.elasticsearch.cluster.{ ClusterService, ClusterState }
import org.elasticsearch.common.Priority

import scala.concurrent.{ Future, Promise }
import scala.util.{ Failure, Success, Try }

object Follower {

  def props(localNode: DiscoveryNode, votingMembers: VotingMembers, clusterService: ClusterService) =
    Props(classOf[Follower], localNode, votingMembers, clusterService)

  case object CheckInitSub

  case class PublishReq(serializedClusterState: ByteString)

  case class LocalMasterPublishNotif(transition: Try[ClusterStateTransition])

  case object WhoYouReq

  case class IAmRsp(ref: ActorRef, node: DiscoveryNode)

}

class Follower(localNode: DiscoveryNode, votingMembers: VotingMembers, clusterService: ClusterService) extends Actor with ActorLogging {

  import Follower._
  import context.dispatcher

  val cluster = Cluster(context.system)

  val firstSubmit = Promise[ClusterStateTransition]()

  override def receive = {

    case CheckInitSub =>
      firstSubmit.future pipeTo sender()

    case WhoYouReq =>
      sender() ! IAmRsp(self, localNode)

    case LocalMasterPublishNotif(transition) =>
      log.debug("received local master publish notification")
      firstSubmit.tryComplete(transition)

    case PublishReq(serializedClusterState) =>
      val publishSender = sender()

      Future {
        ClusterStateSerialization.fromBytes(serializedClusterState, localNode)
      } onComplete {

        case Success(updatedState) =>
          require(updatedState.nodes.masterNodeId != localNode.id, "Master's local follower should not receive Publish messages")

          updatedState.status(ClusterState.ClusterStateStatus.RECEIVED)

          log.info("submitting publish of cluster state version {}...", updatedState.version)
          SubmitClusterStateUpdate(clusterService, "follower{master-publish}", Priority.URGENT, _ => updatedState) onComplete {
            res =>
              res match {
                case Success(transition) =>
                  log.debug("successfully submitted cluster state version {}", updatedState.version)
                  publishSender ! PublishAck(localNode, None)
                case Failure(error) =>
                  log.error(error, "failed to submit cluster state version {}", updatedState.version)
                  publishSender ! PublishAck(localNode, Some(error))
              }
              firstSubmit.tryComplete(res)
          }

        case Failure(error) =>
          log.error(error, "failed to deserialize cluster state received from {}", publishSender)
          publishSender ! PublishAck(localNode, Some(error))

      }

  }

}
