package eskka

import java.util.concurrent.TimeUnit

import scala.Some
import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.concurrent.{ Await, Future, Promise }
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

  lazy val eskkaSettings = settings.getByPrefix("discovery.eskka.")

  lazy val nodeId = DiscoveryService.generateNodeId(settings)

  lazy val localNode = new DiscoveryNode(
    settings.get("name"),
    nodeId,
    transport.boundAddress().publishAddress(),
    discoveryNodeService.buildAttributes(),
    version
  )

  private[this] lazy val publishTimeout = Timeout(discoverySettings.getPublishTimeout.getMillis, TimeUnit.MILLISECONDS)

  private[this] lazy val system = ActorSystem(clusterName.value, config = actorSystemConfig)

  private[this] lazy val cluster = Cluster(system)

  private[this] lazy val masterProxy = system.actorOf(ClusterSingletonProxy.defaultProps("/user/singleton-manager/eskka-master", masterRole))

  private[this] lazy val follower = system.actorOf(Props(classOf[Follower], localNode, clusterService, masterProxy), "eskka-follower")

  private[this] val initialStateListeners = mutable.LinkedHashSet[InitialStateDiscoveryListener]()

  private def actorSystemConfig = {
    val clientNode = settings.getAsBoolean("node.client", false)

    val bindHost = networkService.resolveBindHostAddress(eskkaSettings.get("host", "_non_loopback_")).getHostName
    val bindPort = eskkaSettings.getAsInt("port", if (clientNode) 0 else defaultPort)
    val publishHost = networkService.resolvePublishHostAddress(eskkaSettings.get("host", "_local_")).getHostName
    val seedNodes = eskkaSettings.getAsArray("seed_nodes", Array(publishHost)).map(addr => if (addr.contains(':')) addr else s"$addr:$defaultPort")

    ConfigFactory.parseMap(Map(
      "akka.remote.netty.tcp.hostname" -> bindHost,
      "akka.remote.netty.tcp.port" -> bindPort,
      "akka.cluster.seed-nodes" -> ImmutableList.copyOf(seedNodes.map(hostPort => s"akka.tcp://${clusterName.value}@$hostPort")),
      "akka.cluster.roles" -> (if (seedNodes.contains(s"$publishHost:$bindPort"))
        ImmutableList.of(masterRole)
      else ImmutableList.of()),
      s"akka.cluster.role.$masterRole.min-nr-of-members" -> new Integer((seedNodes.size + 1) / 2)
    )).withFallback(ConfigFactory.load())
  }

  private def masterEligible = cluster.selfRoles.contains(masterRole)

  override def doStart() {
    if (masterEligible) {
      system.actorOf(ClusterSingletonManager.props(
        singletonProps = Props(classOf[Master], localNode, clusterService),
        singletonName = "eskka-master",
        terminationMessage = PoisonPill,
        role = Some(masterRole)
      ), name = "singleton-manager")
    }

    import scala.concurrent.ExecutionContext.Implicits.global
    implicit val timeout = publishTimeout

    val checkInit = follower ? Protocol.CheckInit
    (if (masterEligible) {
      // on a master node, follower won't process publsh
      Future.firstCompletedOf(Seq(masterProxy ? Protocol.QualifiedCheckInit(localNode.id), checkInit))
    } else {
      checkInit
    }).onComplete({
      case Success(transition) =>
        initialStateListeners.foreach(_.initialStateProcessed())
        logger.info("Initial state processed -- {}", transition.asInstanceOf[Object])
      case Failure(e) =>
        logger.error("Initial state processing failed!", e)
    })
  }

  override def doStop() {
    logger.info("Leaving the cluster")
    val p = Promise[Any]() // FIXME Can this be accomplished more cleanly?
    cluster.subscribe(system.actorOf(Props(new Actor {
      override def receive = {
        case ClusterEvent.MemberExited(m) if m.address == cluster.selfAddress =>
          p.success(Nil)
          context.stop(self)
      }
    })), classOf[ClusterEvent.MemberEvent])
    cluster.leave(cluster.selfAddress)
    Await.ready(p.future, Duration.Inf)
  }

  override def doClose() {
    system.shutdown()
    system.awaitTermination()
  }

  override def addListener(listener: InitialStateDiscoveryListener) {
    initialStateListeners += listener
  }

  override def removeListener(listener: InitialStateDiscoveryListener) {
    initialStateListeners -= listener
  }

  override def nodeDescription = clusterName.value + "/" + nodeId

  override def publish(clusterState: ClusterState, ackListener: AckListener) {
    logger.info("Publishing new clusterState [{}]", clusterState)
    masterProxy ! Protocol.Publish(ClusterState.Builder.toBytes(clusterState), system.actorOf(Props(classOf[PublishResponseHandler], ackListener)))
  }

  override def setNodeService(nodeService: NodeService) {
  }

  override def setAllocationService(allocationService: AllocationService) {
  }

}

object EskkaDiscovery {

  val masterRole = "seed"

  val defaultPort = 10300

  private class PublishResponseHandler(ackListener: AckListener) extends Actor {

    import scala.concurrent.ExecutionContext.Implicits.global

    context.system.scheduler.scheduleOnce(Duration(60, TimeUnit.SECONDS), self, PoisonPill)

    var expectedAcks = 0
    var acksReceived = 0

    override def receive = {

      case expectedAcks: Int =>
        this.expectedAcks = expectedAcks
        if (acksReceived > 0 && acksReceived == expectedAcks) {
          context.stop(self)
        }

      case Protocol.PublishAck(node, error) =>
        ackListener.onNodeAck(node, error.orNull)
        acksReceived += 1
        if (acksReceived == expectedAcks) {
          context.stop(self)
        }

    }

  }

}

