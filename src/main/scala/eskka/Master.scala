package eskka

import java.util.concurrent.TimeUnit

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.{ Failure, Success }

import akka.actor._
import akka.cluster.{ Cluster, ClusterEvent }
import akka.pattern.ask
import akka.util.Timeout

import org.elasticsearch.Version
import org.elasticsearch.cluster.{ ClusterService, ClusterState }
import org.elasticsearch.cluster.block.ClusterBlocks
import org.elasticsearch.cluster.node.{ DiscoveryNode, DiscoveryNodes }
import org.elasticsearch.cluster.routing.allocation.AllocationService
import org.elasticsearch.discovery.Discovery

object Master {

  def props(localNode: DiscoveryNode, votingMembers: VotingMembers, version: Version, clusterService: ClusterService, allocationService: AllocationService) =
    Props(classOf[Master], localNode, votingMembers, version, clusterService, allocationService)

  private val MasterDiscoveryDrainInterval = Duration(1, TimeUnit.SECONDS)
  private val WhoYouTimeout = Timeout(500, TimeUnit.MILLISECONDS)

  private case object DrainQueuedDiscoverySubmits

  private case class EnqueueDiscoverySubmit(info: String)

  private case class RetryAddFollower(node: Address)

}

class Master(localNode: DiscoveryNode, votingMembers: VotingMembers, version: Version, clusterService: ClusterService, allocationService: AllocationService)
  extends Actor with ActorLogging {

  import Master._

  import context.dispatcher

  val cluster = Cluster(context.system)

  val drainage = context.system.scheduler.schedule(MasterDiscoveryDrainInterval, MasterDiscoveryDrainInterval, self, DrainQueuedDiscoverySubmits)

  var discoveredNodes = Map[Address, Future[Protocol.IAm]]()

  var pendingDiscoverySubmits = immutable.Queue[String]()

  override def preStart() {
    log.info("Master actor starting up on node [{}]", localNode)
    cluster.subscribe(self, ClusterEvent.InitialStateAsEvents, classOf[ClusterEvent.MemberEvent], classOf[ClusterEvent.ReachabilityEvent])
  }

  override def postStop() {
    cluster.unsubscribe(self)
    drainage.cancel()
    log.info("Master actor stopped on node [{}]", localNode)
  }

  override def receive = {

    case Protocol.MasterPublish(clusterState) =>
      if (votingMembers.quorumAvailable(cluster.state)) {
        val currentRemoteFollowers = remoteFollowers
        if (!currentRemoteFollowers.isEmpty) {
          log.info("publishing cluster state version [{}] to [{}]", clusterState.version, currentRemoteFollowers.mkString(","))
          val requiredVersions = currentRemoteFollowers.map(_.node.version).toSet
          val serializedStates = requiredVersions.map(v => v -> ClusterStateSerialization.toBytes(v, clusterState)).toMap
          for (follower <- currentRemoteFollowers) {
            follower.ref forward Protocol.FollowerPublish(version, serializedStates(follower.node.version))
          }
        }
      } else {
        log.warning("don't have quorum so won't forward publish message for cluster state version [{}]", clusterState.version)
        sender() ! Protocol.PublishAck(localNode, Some(new Protocol.QuorumUnavailable))
      }

    case DrainQueuedDiscoverySubmits =>
      if (!pendingDiscoverySubmits.isEmpty) {
        val summary = pendingDiscoverySubmits.mkString("[", " :: ", "]")
        if (votingMembers.quorumAvailable(cluster.state)) {
          val submission = SubmitClusterStateUpdate(clusterService, s"eskka-master$summary", discoveryState)
          submission onComplete {
            res =>
              res match {
                case Success(transition) =>
                  log.debug("drain discovery submits -- successful -- {}", summary)
                case Failure(e) =>
                  log.error(e, "drain discovery submits -- failure, will retry -- {}", summary)
                  self ! EnqueueDiscoverySubmit("retry")
              }
              localFollower.foreach(_.ref ! Protocol.LocalMasterPublishNotification(res))
          }
        } else {
          log.warning("don't have quorum to submit pending discovery changes {}", summary)
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

    case rEvent: ClusterEvent.ReachabilityEvent => rEvent match {

      case ClusterEvent.UnreachableMember(m) =>

      case ClusterEvent.ReachableMember(m) =>
        // a) quorum checks takes reachability into account -- we may have regained quorum upon a member becoming reachable
        // b) m's follower actor could probably do with receiving a fresh publish
        self ! EnqueueDiscoverySubmit(s"reachable(${m.address})")

    }

    case RetryAddFollower(node) =>
      if (discoveredNodes contains node) {
        addFollower(node)
      }

    case Protocol.PleasePublishDiscoveryState(requestor) =>
      self ! EnqueueDiscoverySubmit(s"request($requestor)")

    case EnqueueDiscoverySubmit(info) =>
      pendingDiscoverySubmits = pendingDiscoverySubmits enqueue info

  }

  def addFollower(node: Address) {
    implicit val timeout = WhoYouTimeout
    val future = (context.actorSelection(RootActorPath(node) / "user" / ActorNames.Follower) ? Protocol.WhoYou).mapTo[Protocol.IAm]
    discoveredNodes += (node -> future)
    future onComplete {
      case Success(_) => self ! EnqueueDiscoverySubmit(s"identified($node)")
      case Failure(_) => self ! RetryAddFollower(node)
    }
  }

  def localFollower: Option[Protocol.IAm] =
    discoveredNodes.get(cluster.selfAddress).flatMap(_.value.collect {
      case Success(iam) => iam
    })

  def remoteFollowers: Iterable[Protocol.IAm] =
    discoveredNodes.filterKeys(_ != cluster.selfAddress).values.flatMap(_.value.collect {
      case Success(iam) => iam
    })

  def discoveryState(currentState: ClusterState): ClusterState = {
    val newState = ClusterState.builder(currentState)
      .nodes(addDiscoveredNodes(DiscoveryNodes.builder.put(localNode).localNodeId(localNode.id).masterNodeId(localNode.id)))
      .blocks(ClusterBlocks.builder.blocks(currentState.blocks).removeGlobalBlock(Discovery.NO_MASTER_BLOCK).build)
      .build

    if (newState.nodes.size < currentState.nodes.size) {
      // @see ZenDiscovery handleLeaveRequest() handleNodeFailure()
      // eagerly run reroute to remove dead nodes from routing table
      ClusterState.builder(newState).routingResult(allocationService.reroute(newState)).build
    } else {
      newState
    }
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
