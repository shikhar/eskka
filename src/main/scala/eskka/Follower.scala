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

  case class LocalMasterPublishNotif(transition: Try[ClusterStateTransition])

  case class AnnounceMaster(address: Address, node: DiscoveryNode)

  case class MasterAck(ref: ActorRef, node: DiscoveryNode)

  case class PublishReqChunk(version: Long, numChunks: Int, chunkSeq: Int, data: ByteString)

  private case class PublishReq(sender: ActorRef, version: Long, totalChunks: Int, receivedChunks: Int, data: ByteString)

}

class Follower(localNode: DiscoveryNode, votingMembers: VotingMembers, clusterService: ClusterService) extends Actor with ActorLogging {

  import Follower._
  import context.dispatcher

  val cluster = Cluster(context.system)

  val firstSubmit = Promise[ClusterStateTransition]()

  var currentMaster: Option[AnnounceMaster] = None

  private var activePublish: Option[PublishReq] = None

  override def receive = {

    case CheckInitSub =>
      firstSubmit.future pipeTo sender()

    case am @ AnnounceMaster(address, node) =>
      log.info("master announced [{}]", am)
      currentMaster = Some(am)
      sender() ! MasterAck(self, localNode)

    case LocalMasterPublishNotif(transition) =>
      log.debug("received local master publish notification")
      firstSubmit.tryComplete(transition)

    case PublishReqChunk(version, totalChunks, chunkSeq, data) if currentMaster.exists(_.address == sender().path.address) =>
      log.debug("received chunk {} of {} for cluster state version {}", chunkSeq + 1, totalChunks, version)

      if (chunkSeq == 0) {

        activePublish = Some(PublishReq(sender(), version, totalChunks, 1, data))

      } else {

        activePublish = activePublish match {
          case Some(acc) =>
            if (sender() == acc.sender && version == acc.version && totalChunks == acc.totalChunks && chunkSeq == acc.receivedChunks) {
              Some(acc.copy(receivedChunks = acc.receivedChunks + 1, data = acc.data ++ data))
            } else {
              sender() ! PublishAck(localNode, Some(new IllegalStateException("Invalid publish chunk")))
              None
            }
          case None =>
            sender() ! PublishAck(localNode, Some(new IllegalStateException("Invalid publish chunk")))
            None
        }

      }

      activePublish = activePublish match {
        case Some(acc) if acc.receivedChunks == acc.totalChunks =>
          self ! acc
          None
        case x =>
          x
      }

    case PublishReq(sender, version, totalChunks, receivedChunks, data) =>
      assume(totalChunks == receivedChunks)

      Future {
        val cs = ClusterStateSerialization.fromBytes(data, localNode)
        require(version == cs.version)
        cs
      } onComplete {

        case Success(updatedState) =>
          updatedState.status(ClusterState.ClusterStateStatus.RECEIVED)

          log.info("submitting publish of cluster state version {} ({}b/{})...", version, data.length, totalChunks)
          SubmitClusterStateUpdate(clusterService, "follower{master-publish}", Priority.URGENT, _ => updatedState) onComplete {
            res =>
              res match {
                case Success(transition) =>
                  log.debug("successfully submitted cluster state version {}", version)
                  sender ! PublishAck(localNode, None)
                case Failure(error) =>
                  log.error(error, "failed to submit cluster state version {}", version)
                  sender ! PublishAck(localNode, Some(error))
              }
              firstSubmit.tryComplete(res)
          }

        case Failure(error) =>
          log.error(error, "failed to deserialize cluster state received from {}", sender)
          sender ! PublishAck(localNode, Some(error))

      }

  }

}
