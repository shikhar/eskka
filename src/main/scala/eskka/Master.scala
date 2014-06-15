package eskka

import java.util.concurrent.TimeUnit

import akka.actor._
import akka.cluster.{ Cluster, ClusterEvent }
import akka.pattern.ask
import akka.util.Timeout
import org.elasticsearch.Version
import org.elasticsearch.cluster.block.ClusterBlocks
import org.elasticsearch.cluster.node.{ DiscoveryNode, DiscoveryNodes }
import org.elasticsearch.cluster.routing.allocation.AllocationService
import org.elasticsearch.cluster.{ ClusterService, ClusterState }
import org.elasticsearch.common.Priority
import org.elasticsearch.discovery.Discovery

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.{ Failure, Success }

object Master {

  def props(localNode: DiscoveryNode, votingMembers: VotingMembers, version: Version, clusterService: ClusterService) =
    Props(classOf[Master], localNode, votingMembers, version, clusterService)

  private val MasterDiscoveryDrainInterval = Duration(1, TimeUnit.SECONDS)
  private val WhoYouTimeout = Timeout(500, TimeUnit.MILLISECONDS)

  private case object DrainQueuedDiscoverySubmits

  private case class EnqueueDiscoverySubmit(info: String)

  private case class RetryAddFollower(node: Address)

  case class PublishReq(clusterState: ClusterState)

}

class Master(localNode: DiscoveryNode, votingMembers: VotingMembers, version: Version, clusterService: ClusterService)
  extends Actor with ActorLogging {

  import Master._
  import context.dispatcher

  val cluster = Cluster(context.system)

  val drainage = context.system.scheduler.schedule(MasterDiscoveryDrainInterval, MasterDiscoveryDrainInterval, self, DrainQueuedDiscoverySubmits)

  var discoveredNodes = Map[Address, Future[Follower.IAmRsp]]()

  var pendingDiscoverySubmits = immutable.Queue[String]()

  override def preStart() {
    log.debug("Master actor starting up on node [{}]", localNode)
    cluster.subscribe(self, ClusterEvent.InitialStateAsEvents, classOf[ClusterEvent.MemberEvent])
  }

  override def postStop() {
    cluster.unsubscribe(self)
    drainage.cancel()
    log.debug("Master actor stopped on node [{}]", localNode)
  }

  override def receive = {

    case PublishReq(clusterState) =>
      val publishSender = sender()
      val currentRemoteFollowers = remoteFollowers
      if (currentRemoteFollowers.nonEmpty) {
        val requiredEsVersions = currentRemoteFollowers.map(_.node.version).toSet
        Future {
          requiredEsVersions.map(v => v -> ClusterStateSerialization.toBytes(v, clusterState)).toMap
        } onComplete {
          case Success(serializedStates) =>
            log.info("publishing cluster state version [{}] to [{}]", clusterState.version, currentRemoteFollowers.map(_.ref.path).mkString(","))
            for (follower <- currentRemoteFollowers) {
              val followerEsVersion = follower.node.version
              follower.ref.tell(Follower.PublishReq(followerEsVersion, serializedStates(followerEsVersion)), publishSender)
            }
          case Failure(error) =>
            log.error(error, "failed to serialize cluster state version {} for elasticsearch versions {}", clusterState.version, requiredEsVersions)
            publishSender ! PublishAck(localNode, Some(error))
        }
      }

    case DrainQueuedDiscoverySubmits =>
      if (pendingDiscoverySubmits.nonEmpty) {
        val summary = pendingDiscoverySubmits.mkString("[", " :: ", "]")
        val submission = SubmitClusterStateUpdate(clusterService, s"eskka-master-$summary", Priority.IMMEDIATE, discoveryState)
        submission onComplete {
          res =>
            res match {
              case Success(transition) =>
                log.debug("drain discovery submits -- successful -- {}", summary)
              case Failure(e) =>
                log.error(e, "drain discovery submits -- failure, will retry -- {}", summary)
                context.system.scheduler.scheduleOnce(MasterDiscoveryDrainInterval, self, EnqueueDiscoverySubmit("retry"))
            }
            localFollower.foreach(_.ref ! Follower.LocalMasterPublishNotif(res))
        }
        pendingDiscoverySubmits = immutable.Queue()
      }

    case mEvent: ClusterEvent.MemberEvent => mEvent match {

      case ClusterEvent.MemberUp(m) =>
        addFollower(m.address)

      case ClusterEvent.MemberExited(m) =>
        if (discoveredNodes.contains(m.address)) {
          discoveredNodes -= m.address
          self ! EnqueueDiscoverySubmit(s"exited(${m.address})")
        }

      case ClusterEvent.MemberRemoved(m, _) =>
        if (discoveredNodes.contains(m.address)) {
          discoveredNodes -= m.address
          self ! EnqueueDiscoverySubmit(s"removed(${m.address})")
        }

    }

    case RetryAddFollower(node) =>
      if (discoveredNodes contains node) {
        addFollower(node)
      }

    case EnqueueDiscoverySubmit(info) =>
      pendingDiscoverySubmits = pendingDiscoverySubmits enqueue info

  }

  def addFollower(node: Address) {
    implicit val timeout = WhoYouTimeout
    val future = (context.actorSelection(RootActorPath(node) / "user" / ActorNames.Follower) ? Follower.WhoYouReq).mapTo[Follower.IAmRsp]
    discoveredNodes += (node -> future)
    future onComplete {
      case Success(_) => self ! EnqueueDiscoverySubmit(s"identified($node)")
      case Failure(_) => context.system.scheduler.scheduleOnce(WhoYouTimeout.duration, self, RetryAddFollower(node))
    }
  }

  def localFollower: Option[Follower.IAmRsp] =
    discoveredNodes.get(cluster.selfAddress).flatMap(_.value.collect {
      case Success(iam) => iam
    })

  def remoteFollowers: Iterable[Follower.IAmRsp] =
    discoveredNodes.filterKeys(_ != cluster.selfAddress).values.flatMap(_.value.collect {
      case Success(iam) => iam
    })

  def discoveryState(currentState: ClusterState): ClusterState = {
    ClusterState.builder(currentState)
      .nodes(addDiscoveredNodes(DiscoveryNodes.builder.put(localNode).localNodeId(localNode.id).masterNodeId(localNode.id)))
      .blocks(ClusterBlocks.builder.blocks(currentState.blocks).removeGlobalBlock(Discovery.NO_MASTER_BLOCK).build)
      .build
  }

  def addDiscoveredNodes(builder: DiscoveryNodes.Builder) = {
    for {
      iAmFuture <- discoveredNodes.values
      Success(iam) <- iAmFuture.value
    } {
      builder.put(iam.node)
    }
    builder
  }

}
