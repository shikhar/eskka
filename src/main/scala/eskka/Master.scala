package eskka

import java.util.concurrent.TimeUnit

import scala.collection.immutable
import concurrent.{ Promise, Future }
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
import org.elasticsearch.common.Priority
import org.elasticsearch.discovery.Discovery
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.{ TransportConnectionListener, TransportService }

object Master {

  def props(localNode: DiscoveryNode, votingMembers: VotingMembers, version: Version,
            threadPool: ThreadPool, clusterService: ClusterService, transportService: TransportService) =
    Props(classOf[Master], localNode, votingMembers, version, threadPool, clusterService, transportService)

  private val MasterDiscoveryDrainInterval = Duration(1, TimeUnit.SECONDS)
  private val WhoYouTimeout = Timeout(500, TimeUnit.MILLISECONDS)
  private val ReconnectInterval = Duration(5, TimeUnit.SECONDS)

  private case object DrainQueuedDiscoverySubmits

  private case class DiscoverySubmit(key: String, info: String)

  private case class RetryAddFollower(node: Address)

  private sealed trait TransportEvent {
    def node: DiscoveryNode
  }

  private case class TransportConnected(node: DiscoveryNode) extends TransportEvent

  private case class TransportDisconnected(node: DiscoveryNode) extends TransportEvent

  private case class Connect(node: DiscoveryNode, isReconnect: Boolean = false)

  case class PublishReq(clusterState: ClusterState)

}

class Master(localNode: DiscoveryNode, votingMembers: VotingMembers, version: Version,
             threadPool: ThreadPool, clusterService: ClusterService, transportService: TransportService)
  extends Actor with ActorLogging with TransportConnectionListener {

  import Master._
  import context.dispatcher

  private val cluster = Cluster(context.system)

  private val drainage = context.system.scheduler.schedule(MasterDiscoveryDrainInterval, MasterDiscoveryDrainInterval, self, DrainQueuedDiscoverySubmits)

  private var discoveredNodes = Map[Address, Future[Follower.IAmRsp]]()

  private var pendingDiscoverySubmits = immutable.Queue[DiscoverySubmit]()

  private var submitCounter = 0

  override def preStart() {
    log.debug("Master actor starting up on node [{}]", localNode)
    cluster.subscribe(self, ClusterEvent.InitialStateAsEvents, classOf[ClusterEvent.MemberEvent])
    transportService.addConnectionListener(this)
  }

  override def postStop() {
    transportService.removeConnectionListener(this)
    cluster.unsubscribe(self)
    drainage.cancel()
    log.debug("Master actor stopped on node [{}]", localNode)
  }

  override def receive = {

    case PublishReq(clusterState) =>
      val publishSender = sender()
      val currentRemoteFollowers = remoteFollowers.filter(iam => clusterState.nodes.nodes.containsKey(iam.node.id))
      if (currentRemoteFollowers.nonEmpty) {
        val requiredEsVersions = currentRemoteFollowers.map(_.node.version).toSet
        Future {
          requiredEsVersions.map(v => v -> ClusterStateSerialization.toBytes(v, clusterState)).toMap
        } onComplete {
          case Success(serializedStates) =>
            val followerAddresses = currentRemoteFollowers.map(_.ref.path.address.hostPort)
            log.info("publishing cluster state version [{}] to [{}]", clusterState.version, followerAddresses.mkString(","))
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

        submitCounter += 1

        val submitRef = submitCounter.toString

        val summary = (for {
          (key, submits) <- pendingDiscoverySubmits.toSet.groupBy { submit: DiscoverySubmit => submit.key }
          infos = submits.map(_.info).mkString(",")
        } yield s"$infos($key})")
          .mkString("[", " :: ", "]")

        val submission = SubmitClusterStateUpdate(clusterService, s"eskka-master-$submitRef-$summary", Priority.IMMEDIATE, discoveryState)
        submission onComplete {
          res =>
            res match {
              case Success(transition) =>
                log.debug("drain discovery submits -- successful -- {}", summary)
              case Failure(e) =>
                log.error(e, "drain discovery submits -- failure, will retry -- {}", summary)
                context.system.scheduler.scheduleOnce(MasterDiscoveryDrainInterval, self, DiscoverySubmit(submitRef, "retry"))
            }
            localFollower.foreach(_.ref ! Follower.LocalMasterPublishNotif(res))
        }

        pendingDiscoverySubmits = immutable.Queue()
      }

    case mEvent: ClusterEvent.MemberEvent => mEvent match {

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

    case tEvent: TransportEvent => tEvent match {

      case TransportConnected(node) =>
        membersByDiscoveryNode.get(node).foreach(address => {
          self ! DiscoverySubmit(address.hostPort, "connected")
        })

      case TransportDisconnected(node) =>
        membersByDiscoveryNode.get(node).foreach(address => {
          self ! DiscoverySubmit(address.hostPort, "disconnected")
          self ! Connect(node, isReconnect = true)
        })

    }

    case Connect(node, isReconnect) =>
      membersByDiscoveryNode.get(node).foreach(address => {
        asyncConnectToNode(node) onFailure {
          case error =>
            if (!isReconnect) {
              log.error(error, "failed to connect to {} ({})", node, address)
            } else {
              log.debug("failed to reconnect to {} ({}) due to {}", node, address, error.getMessage)
            }
            context.system.scheduler.scheduleOnce(ReconnectInterval, self, Connect(node, isReconnect = true))
        }
      })

    case RetryAddFollower(node) =>
      if (discoveredNodes contains node) {
        addFollower(node)
      }

    case submit: DiscoverySubmit =>
      pendingDiscoverySubmits = pendingDiscoverySubmits enqueue submit

  }

  def addFollower(address: Address) {
    implicit val timeout = WhoYouTimeout
    val future = (context.actorSelection(RootActorPath(address) / "user" / ActorNames.Follower) ? Follower.WhoYouReq).mapTo[Follower.IAmRsp]
    discoveredNodes += (address -> future)
    future onComplete {
      case Success(rsp) =>
        self ! Connect(rsp.node)
        self ! DiscoverySubmit(address.hostPort, "identified")
      case Failure(_) =>
        context.system.scheduler.scheduleOnce(WhoYouTimeout.duration, self, RetryAddFollower(address))
    }
  }

  def membersByDiscoveryNode: Map[DiscoveryNode, Address] = {
    for {
      (address, iAmFuture) <- discoveredNodes
      Success(iam) <- iAmFuture.value
    } yield (iam.node, address)
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
      if transportService.nodeConnected(iam.node)
    } {
      builder.put(iam.node)
    }
    builder
  }

  def asyncConnectToNode(node: DiscoveryNode): Future[DiscoveryNode] = {
    val p = Promise[DiscoveryNode]()
    threadPool.generic().execute(new Runnable {
      override def run() {
        try {
          transportService.connectToNode(node)
          p.success(node)
        } catch {
          case t: Throwable => p.failure(t)
        }
      }
    })
    p.future
  }

  override def onNodeConnected(node: DiscoveryNode) {
    self ! TransportConnected(node)
  }

  override def onNodeDisconnected(node: DiscoveryNode) {
    self ! TransportDisconnected(node)
  }

}
