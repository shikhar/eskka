package eskka

import java.util.concurrent.TimeUnit

import akka.actor._
import akka.cluster.{ Cluster, ClusterEvent }
import akka.pattern.ask
import akka.util.Timeout
import org.elasticsearch.cluster.block.ClusterBlocks
import org.elasticsearch.cluster.node.{ DiscoveryNode, DiscoveryNodes }
import org.elasticsearch.cluster.{ ClusterService, ClusterState }
import org.elasticsearch.common.Priority
import org.elasticsearch.discovery.DiscoverySettings

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.{ Failure, Success, Try }

object Master {

  private val MasterDiscoveryDrainInterval = Duration(1, TimeUnit.SECONDS)
  private val FollowerMasterAckTimeout = Timeout(500, TimeUnit.MILLISECONDS)
  private val MaxPublishChunkSize = 16384

  def props(localNode: DiscoveryNode, votingMembers: VotingMembers, clusterService: ClusterService) =
    Props(classOf[Master], localNode, votingMembers, clusterService)

  case class PublishReq(clusterState: ClusterState)

  private case class DiscoverySubmit(key: String, info: String)

  private case class RetryAddFollower(node: Address)

  private case class FollowerInfo(ref: ActorRef, node: DiscoveryNode)

  private case object DrainQueuedDiscoverySubmits

}

class Master(localNode: DiscoveryNode, votingMembers: VotingMembers, clusterService: ClusterService)
  extends Actor with ActorLogging {

  import context.dispatcher
  import eskka.Master._

  private val serializedLocalNode = DiscoveryNodeSerialization.toBytes(localNode).compact

  private val cluster = Cluster(context.system)

  private val drainage = context.system.scheduler.schedule(MasterDiscoveryDrainInterval, MasterDiscoveryDrainInterval, self, DrainQueuedDiscoverySubmits)

  // The keys represent addresses of all cluster members that are currently UP, and the values our current attempt at getting FollowerInfo from them
  private var discoveredNodes = Map[Address, Future[FollowerInfo]]()

  private var pendingDiscoverySubmits = immutable.Queue[DiscoverySubmit]()

  private var submitCounter = 0

  override def preStart() {
    log.info("Master actor starting up on node [{}]", localNode)
    cluster.subscribe(self, ClusterEvent.InitialStateAsEvents, classOf[ClusterEvent.MemberEvent])
  }

  override def postStop() {
    cluster.unsubscribe(self)
    drainage.cancel()
    log.info("Master actor stopped on node [{}]", localNode)
  }

  override def receive: Actor.Receive = {

    case PublishReq(clusterState) =>
      val publishSender = sender()
      val currentRemoteFollowers = remoteFollowers.filter(followerInfo => clusterState.nodes.nodes.containsKey(followerInfo.node.id))
      if (currentRemoteFollowers.nonEmpty) {
        Try(ClusterStateSerialization.toBytes(clusterState)) match {

          case Success(serializedState) =>

            val chunks = serializedState.grouped(MaxPublishChunkSize).toIndexedSeq
            val numChunks = chunks.length

            val followerRefs = currentRemoteFollowers.map(_.ref)
            val followerAddresses = followerRefs.map(_.path.address.hostPort)

            log.info("publishing cluster state version [{}] serialized as [{}b/{}] to [{}]",
              clusterState.version, serializedState.length, numChunks, followerAddresses.mkString(","))
            for {
              chunkSeq <- 0 until numChunks
              publishReqChunk = Follower.PublishReqChunk(clusterState.version, numChunks, chunkSeq, chunks(chunkSeq).compact)
              ref <- followerRefs
            } ref.tell(publishReqChunk, publishSender)

          case Failure(error) =>
            log.error(error, "failed to serialize cluster state version {}", clusterState.version)
            publishSender ! PublishAck(serializedLocalNode, Some(error))

        }
      }

    case DrainQueuedDiscoverySubmits =>
      if (pendingDiscoverySubmits.nonEmpty) {

        submitCounter += 1

        val submitRef = submitCounter.toString

        val summary = (for {
          (key, submits) <- pendingDiscoverySubmits.toSet.groupBy { submit: DiscoverySubmit => submit.key }
          infos = submits.map(_.info).mkString(",")
        } yield s"$infos($key})")
          .mkString("[", " :: ", "]")

        val submission = SubmitClusterStateUpdate(clusterService, s"eskka-master-$submitRef-$summary", Priority.IMMEDIATE,
          runOnlyOnMaster = true, discoveryState)
        submission onComplete {
          res =>
            res match {
              case Success(transition) =>
                log.debug("drain discovery submits -- successful -- {}", summary)
              case Failure(e) =>
                log.error(e, "drain discovery submits -- failure, will retry -- {}", summary)
                context.system.scheduler.scheduleOnce(MasterDiscoveryDrainInterval, self, DiscoverySubmit(submitRef, "retry"))
            }
            localFollower.foreach(_.ref ! Follower.LocalMasterDiscoverySubmitNotif(res))
        }

        pendingDiscoverySubmits = immutable.Queue()
      }

    case mEvent: ClusterEvent.MemberEvent => mEvent match {

      case ClusterEvent.MemberWeaklyUp(m) =>
      // Ignore

      case ClusterEvent.MemberUp(m) =>
        addFollower(m.address)

      case ClusterEvent.MemberExited(m) =>
        if (discoveredNodes contains m.address) {
          discoveredNodes -= m.address
          self ! DiscoverySubmit(m.address.hostPort, "exited")
        }

      case ClusterEvent.MemberRemoved(m, _) =>
        if (discoveredNodes contains m.address) {
          discoveredNodes -= m.address
          self ! DiscoverySubmit(m.address.hostPort, "removed")
        }

    }

    case RetryAddFollower(node) =>
      if (discoveredNodes contains node) {
        addFollower(node)
      }

    case submit: DiscoverySubmit =>
      pendingDiscoverySubmits = pendingDiscoverySubmits enqueue submit

  }

  private def addFollower(address: Address) {
    implicit val timeout = FollowerMasterAckTimeout
    val followerSelection = context.actorSelection(RootActorPath(address) / "user" / ActorNames.Follower)
    val followerInfoFuture = (followerSelection ? Follower.AnnounceMaster(cluster.selfAddress))
      .mapTo[Follower.MasterAck]
      .map(masterAck => FollowerInfo(masterAck.ref, DiscoveryNodeSerialization.fromBytes(masterAck.node)))
    discoveredNodes += (address -> followerInfoFuture)
    followerInfoFuture onComplete {
      case Success(rsp) =>
        self ! DiscoverySubmit(address.hostPort, "identified")
      case Failure(_) =>
        context.system.scheduler.scheduleOnce(FollowerMasterAckTimeout.duration, self, RetryAddFollower(address))
    }
  }

  private def localFollower: Option[FollowerInfo] =
    discoveredNodes.get(cluster.selfAddress).flatMap(_.value.collect {
      case Success(followerInfo) => followerInfo
    })

  private def remoteFollowers: Iterable[FollowerInfo] =
    discoveredNodes.filterKeys(_ != cluster.selfAddress).values.flatMap(_.value.collect {
      case Success(followerInfo) => followerInfo
    })

  private def discoveryState(currentState: ClusterState): ClusterState = {
    val nodes = addDiscoveredNodes(DiscoveryNodes.builder.put(localNode).localNodeId(localNode.id).masterNodeId(localNode.id))
    val blocks = ClusterBlocks.builder
      .blocks(currentState.blocks)
      .removeGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ALL)
      .removeGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_WRITES)
      .build
    ClusterState.builder(currentState)
      .nodes(nodes)
      .blocks(blocks)
      .build
  }

  private def addDiscoveredNodes(builder: DiscoveryNodes.Builder): DiscoveryNodes.Builder = {
    for {
      followerInfoFuture <- discoveredNodes.values
      Success(followerInfo) <- followerInfoFuture.value
    } {
      builder.put(followerInfo.node)
    }
    builder
  }

}
