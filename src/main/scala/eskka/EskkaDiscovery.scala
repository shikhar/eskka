package eskka

import java.util.concurrent.TimeUnit

import scala.Some
import scala.collection.{immutable, mutable}
import scala.collection.JavaConversions._
import scala.util.{Failure, Success}

import akka.actor._
import akka.cluster.Cluster
import akka.contrib.pattern.{ClusterSingletonManager, ClusterSingletonProxy}
import akka.pattern.ask
import akka.util.Timeout

import com.google.common.collect.ImmutableList
import com.typesafe.config.ConfigFactory
import org.elasticsearch.Version
import org.elasticsearch.cluster.{ClusterName, ClusterService, ClusterState}
import org.elasticsearch.cluster.node.{DiscoveryNode, DiscoveryNodeService}
import org.elasticsearch.cluster.routing.allocation.AllocationService
import org.elasticsearch.common.component.AbstractLifecycleComponent
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.discovery.{Discovery, DiscoveryService, DiscoverySettings, InitialStateDiscoveryListener}
import org.elasticsearch.discovery.Discovery.AckListener
import org.elasticsearch.node.service.NodeService
import org.elasticsearch.transport.TransportService

class EskkaDiscovery(private[this] val settings: Settings,
  private[this] val clusterName: ClusterName,
  private[this] val transportService: TransportService,
  private[this] val clusterService: ClusterService,
  private[this] val discoveryNodeService: DiscoveryNodeService,
  private[this] val discoverySettings: DiscoverySettings,
  private[this] val version: Version)
    extends AbstractLifecycleComponent[Discovery](settings) with Discovery {

  val seedNodes = immutable.Seq() ++ settings.getAsArray("seed_nodes").map(AddressFromURIString(_))

  val nodeId = DiscoveryService.generateNodeId(settings)

  val localNode = new DiscoveryNode(
    settings.get("name"),
    nodeId,
    transportService.boundAddress().publishAddress(),
    discoveryNodeService.buildAttributes(),
    version
  )

  private[this] val publishTimeout = Timeout(discoverySettings.getPublishTimeout.getMillis, TimeUnit.MILLISECONDS)

  private[this] val masterRole = "seed"

  private[this] val system = ActorSystem(
    clusterName + "-eskka",
    config = ConfigFactory.parseMap(Map(
      "akka.cluster.roles" -> (if (seedNodes.contains(localNode.name())) ImmutableList.of(masterRole) else ImmutableList.of()),
      s"akka.cluster.role.$masterRole.min-nr-of-members" -> new Integer((seedNodes.size + 1) / 2)
    )).withFallback(ConfigFactory.load())
  )

  private[this] val cluster = Cluster(system)

  private[this] val singletonManager = system.actorOf(ClusterSingletonManager.props(
    singletonProps = Props(classOf[Master], localNode, clusterService),
    singletonName = "eskka-master",
    terminationMessage = PoisonPill,
    role = Some(masterRole)
  ), name = "singleton-manager")

  private[this] val masterProxy = system.actorOf(ClusterSingletonProxy.defaultProps("/user/singleton-manager/eskka-master", masterRole))

  private[this] val follower = system.actorOf(Props(classOf[Follower], localNode, clusterService))

  private[this] val initialStateListeners = mutable.LinkedHashSet[InitialStateDiscoveryListener]()

  override def doStart() {
    import scala.concurrent.ExecutionContext.Implicits.global
    cluster.registerOnMemberUp({
      (masterProxy ? Protocol.Init)(publishTimeout).onComplete({
        case Success(transition) =>
          initialStateListeners.foreach(_.initialStateProcessed())
          logger.info("Initial state processed, transition={}", transition.asInstanceOf[Object])
        case Failure(e) =>
          logger.error("Initial state processing failed!", e)
      })
    })
    cluster.joinSeedNodes(seedNodes)
  }

  override def doStop() {
    cluster.leave(cluster.selfAddress)
  }

  override def doClose() {
    system.shutdown()
  }

  override def addListener(listener: InitialStateDiscoveryListener) {
    initialStateListeners += listener
  }

  override def removeListener(listener: InitialStateDiscoveryListener) {
    initialStateListeners -= listener
  }

  override def nodeDescription = clusterName.value + "/" + nodeId

  override def publish(clusterState: ClusterState, ackListener: AckListener) {
    masterProxy ! Protocol.Publish(clusterState, system.actorOf(Props(classOf[EskkaDiscovery.PublishResponseHandler], ackListener)))
  }

  override def setNodeService(nodeService: NodeService) {
  }

  override def setAllocationService(allocationService: AllocationService) {
  }

}

object EskkaDiscovery {

  private class PublishResponseHandler(ackListener: AckListener) extends Actor {
    override def receive = {
      case Protocol.PublishAck(node, error) =>
        ackListener.onNodeAck(node, error.get)
    }
  }

}