package eskka

import java.util.concurrent.TimeUnit

import scala.Some
import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.concurrent.{ Await, Future, Promise, TimeoutException }
import scala.concurrent.duration.Duration
import scala.util.{ Failure, Success }

import akka.actor._
import akka.cluster.{ Cluster, ClusterEvent }
import akka.contrib.pattern.{ ClusterSingletonManager, ClusterSingletonProxy }
import akka.pattern.ask
import akka.util.Timeout

import com.google.common.collect.ImmutableList
import com.typesafe.config.ConfigFactory
import org.elasticsearch.Version
import org.elasticsearch.cluster.{ ClusterName, ClusterService, ClusterState }
import org.elasticsearch.cluster.node.{ DiscoveryNode, DiscoveryNodeService }
import org.elasticsearch.cluster.routing.allocation.AllocationService
import org.elasticsearch.common.component.AbstractLifecycleComponent
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.network.NetworkService
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.discovery.{ Discovery, DiscoveryService, DiscoverySettings, InitialStateDiscoveryListener }
import org.elasticsearch.discovery.Discovery.AckListener
import org.elasticsearch.node.service.NodeService
import org.elasticsearch.transport.Transport

class EskkaDiscovery @Inject() (private[this] val settings: Settings,
                                private[this] val clusterName: ClusterName,
                                private[this] val transport: Transport,
                                private[this] val networkService: NetworkService,
                                private[this] val clusterService: ClusterService,
                                private[this] val discoveryNodeService: DiscoveryNodeService,
                                private[this] val discoverySettings: DiscoverySettings,
                                private[this] val version: Version)
  extends AbstractLifecycleComponent[Discovery](settings) with Discovery {

  import EskkaDiscovery._

  private[this] lazy val nodeId = DiscoveryService.generateNodeId(settings)

  private[this] lazy val system = makeActorSystem()
  private[this] lazy val cluster = Cluster(system)

  private[this] var allocationService: AllocationService = null

  private[this] val initialStateListeners = mutable.LinkedHashSet[InitialStateDiscoveryListener]()

  override def addListener(listener: InitialStateDiscoveryListener) {
    initialStateListeners += listener
  }

  override def removeListener(listener: InitialStateDiscoveryListener) {
    initialStateListeners -= listener
  }

  override def doStart() {
    require(allocationService != null)

    if (cluster.settings.SeedNodes.size == 1) {
      logger.warn("Highly recommended to configure more than one seed node using `eskka.seed_nodes`")
    }

    val votingMembers = VotingMembers(cluster.settings.SeedNodes.toSet)

    cluster.registerOnMemberUp {

      if (cluster.selfRoles.contains(MasterRole)) {
        system.actorOf(ClusterSingletonManager.props(
          singletonProps = Master.props(localNode, votingMembers, clusterService, allocationService),
          singletonName = ActorNames.Master,
          terminationMessage = PoisonPill,
          role = Some(MasterRole)), name = ActorNames.CSM)
      }

      system.actorOf(Pinger.props, ActorNames.Pinger)

      if (votingMembers.addresses(cluster.selfAddress)) {
        system.actorOf(QuorumBasedPartitionMonitor.props(votingMembers, partitionEvalDelay, partitionPingTimeout), "partition-monitor")
      }

      val follower = system.actorOf(Follower.props(localNode, votingMembers, clusterService,
        ClusterSingletonProxy.defaultProps(s"/user/${ActorNames.CSM}/${ActorNames.Master}", MasterRole)),
        ActorNames.Follower)

      import scala.concurrent.ExecutionContext.Implicits.global
      implicit val timeout = Timeout(discoverySettings.getPublishTimeout.getMillis, TimeUnit.MILLISECONDS)
      (follower ? Protocol.CheckInit) onComplete {
        case Success(info) =>
          initialStateListeners.foreach(_.initialStateProcessed())
          logger.info("Initial state processed -- {}", info.asInstanceOf[Object])
        case Failure(e) =>
          logger.error("Initial state processing failed!", e)
      }

    }
  }

  override def doStop() {
    logger.info("Leaving the cluster")
    val p = Promise[Any]()
    cluster.subscribe(system.actorOf(Props(new Actor {
      override def receive = {
        case ClusterEvent.MemberRemoved(m, _) if m.address == cluster.selfAddress =>
          p.success(Nil)
          context.stop(self)
      }
    })), classOf[ClusterEvent.MemberEvent])
    cluster.leave(cluster.selfAddress)
    Await.ready(p.future, ShutdownTimeout.duration)
  }

  override def doClose() {
    system.shutdown()
    system.awaitTermination()
  }

  override lazy val localNode = new DiscoveryNode(
    settings.get("name"),
    nodeId,
    transport.boundAddress().publishAddress(),
    discoveryNodeService.buildAttributes() + ("eskka_address" -> cluster.selfAddress.toString),
    version)

  override def nodeDescription = clusterName.value + "/" + nodeId

  override def publish(clusterState: ClusterState, ackListener: AckListener) {
    logger.trace("publishing new cluster state [{}]", clusterState)

    val publishTimeoutMs = discoverySettings.getPublishTimeout.millis

    val nonMasterNodes = clusterState.nodes.size - 1

    val (publishResponseHandler, fullyAckedFuture) =
      if (nonMasterNodes > 0) {
        implicit val timeout = if (publishTimeoutMs > 0) Timeout(publishTimeoutMs, TimeUnit.MILLISECONDS) else PublishTimeoutHard
        val handler = system.actorOf(Props(classOf[PublishResponseHandler], nonMasterNodes, ackListener, timeout))
        (handler, handler ? PublishResponseHandler.SubscribeFullyAcked)
      } else {
        (Actor.noSender, Future.successful(PublishResponseHandler.FullyAcked))
      }

    system.actorSelection(s"/user/${ActorNames.CSM}/${ActorNames.Master}").tell(Protocol.MasterPublish(clusterState), publishResponseHandler)

    if (publishTimeoutMs > 0) {
      try {
        Await.ready(fullyAckedFuture, Duration(publishTimeoutMs, TimeUnit.MILLISECONDS))
      } catch {
        case e: TimeoutException =>
          logger.warn("timed out waiting for all nodes to acknowledge cluster state version {}", e, clusterState.version.asInstanceOf[Object])
      }
    }
  }

  override def setNodeService(nodeService: NodeService) {
  }

  override def setAllocationService(allocationService: AllocationService) {
    this.allocationService = allocationService
  }

  private def partitionEvalDelay =
    Duration(settings.getAsTime("discovery.eskka.partition.eval-delay", TimeValue.timeValueSeconds(5)).millis(), TimeUnit.MILLISECONDS)

  private def partitionPingTimeout =
    Duration(settings.getAsTime("discovery.eskka.partition.ping-timeout", TimeValue.timeValueSeconds(2)).millis(), TimeUnit.MILLISECONDS)

  private def makeActorSystem() = {
    val name = clusterName.value
    val nodeSettings = settings.getByPrefix("node.")
    val isClientNode = nodeSettings.getAsBoolean("client", false)
    val isMasterNode = nodeSettings.getAsBoolean("master", !isClientNode)

    val eskkaSettings = settings.getByPrefix("discovery.eskka.")

    val hostname = networkService.resolvePublishHostAddress(
      eskkaSettings.get("host", settings.get("transport.bind_host", settings.get("transport.host", "_local_")))).getHostName

    val bindPort = eskkaSettings.getAsInt("port", if (isClientNode) 0 else DefaultPort)

    val seedNodes = eskkaSettings.getAsArray("seed_nodes", Array(hostname)).map(addr => if (addr.contains(':')) addr else s"$addr:$DefaultPort")
    val seedNodeAddresses = ImmutableList.copyOf(seedNodes.map(hostPort => s"akka.tcp://$name@$hostPort"))
    val roles = if (isMasterNode) ImmutableList.of(MasterRole) else ImmutableList.of()
    val minNrOfMembers = new Integer((seedNodes.size / 2) + 1)

    val eskkaConfig = ConfigFactory.parseMap(Map(
      "akka.remote.netty.tcp.hostname" -> hostname,
      "akka.remote.netty.tcp.port" -> bindPort,
      "akka.cluster.seed-nodes" -> seedNodeAddresses,
      "akka.cluster.roles" -> roles,
      "akka.cluster.min-nr-of-members" -> minNrOfMembers))

    logger.info("creating actor system with eskka config {}", eskkaConfig)

    ActorSystem(name, config = eskkaConfig.withFallback(ConfigFactory.load()))
  }

}

object EskkaDiscovery {

  private val DefaultPort = 9400
  private val ShutdownTimeout = Timeout(5, TimeUnit.SECONDS)
  private val PublishTimeoutHard = Timeout(60, TimeUnit.SECONDS)

}
