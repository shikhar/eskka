package eskka

import java.util.concurrent.TimeUnit

import scala.collection.mutable
import scala.collection.JavaConversions._

import akka.actor._
import akka.cluster.Cluster
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
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.discovery.{ Discovery, DiscoveryService, DiscoverySettings, InitialStateDiscoveryListener }
import org.elasticsearch.discovery.Discovery.AckListener
import org.elasticsearch.node.service.NodeService
import org.elasticsearch.transport.TransportService
import scala.concurrent.Future

//import org.elasticsearch.rest._

import scala.util.Failure
import scala.Some
import scala.util.Success

class EskkaDiscovery @Inject() (private[this] val settings: Settings,
  private[this] val clusterName: ClusterName,
  private[this] val transportService: TransportService,
  private[this] val clusterService: ClusterService,
  private[this] val discoveryNodeService: DiscoveryNodeService,
  private[this] val discoverySettings: DiscoverySettings,
  private[this] val version: Version)
    extends AbstractLifecycleComponent[Discovery](settings) with Discovery {

  lazy val eskkaSettings = settings.getByPrefix("discovery.eskka")

  lazy val seedNodes = ImmutableList.copyOf(eskkaSettings.getAsArray(".seed_nodes").map(addr => s"akka.tcp://${clusterName.value}@$addr"))

  lazy val nodeId = DiscoveryService.generateNodeId(settings)

  lazy val localNode = new DiscoveryNode(
    settings.get("name"),
    nodeId,
    transportService.boundAddress().publishAddress(),
    discoveryNodeService.buildAttributes(),
    version
  )

  private[this] val publishTimeout = Timeout(discoverySettings.getPublishTimeout.getMillis, TimeUnit.MILLISECONDS)

  private[this] val masterRole = "seed"

  private[this] lazy val system = ActorSystem(
    clusterName.value,
    config = ConfigFactory.parseMap(Map(
      "akka.remote.netty.tcp.port" -> eskkaSettings.getAsInt(".port", 0),
      "akka.cluster.roles" -> ImmutableList.of(masterRole), // FIXME
      "akka.cluster.seed-nodes" -> seedNodes,
      s"akka.cluster.role.$masterRole.min-nr-of-members" -> new Integer((seedNodes.size + 1) / 2)
    )).withFallback(ConfigFactory.load())
  )

  private[this] lazy val cluster = Cluster(system)

  private[this] lazy val masterProxy = system.actorOf(
    ClusterSingletonProxy.defaultProps("/user/singleton-manager/eskka-master", masterRole)
  )

  private[this] lazy val follower = system.actorOf(Props(classOf[Follower], localNode, clusterService, masterProxy), "eskka-follower")

  private[this] val initialStateListeners = mutable.LinkedHashSet[InitialStateDiscoveryListener]()

  override def doStart() {
    system.actorOf(ClusterSingletonManager.props(
      singletonProps = Props(classOf[Master], localNode, clusterService),
      singletonName = "eskka-master",
      terminationMessage = PoisonPill,
      role = Some(masterRole)
    ), name = "singleton-manager")

    import scala.concurrent.ExecutionContext.Implicits.global
    implicit val timeout = publishTimeout

    Future.firstCompletedOf(Seq(
      masterProxy ? Protocol.QualifiedCheckInit(localNode.id),
      follower ? Protocol.CheckInit
    )).onComplete({
      case Success(transition) =>
        initialStateListeners.foreach(_.initialStateProcessed())
        logger.info("Initial state processed, transition={}", transition.asInstanceOf[Object])
      case Failure(e) =>
        logger.error("Initial state processing failed!", e)
    })
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
    logger.info("Publishing new clusterState [{}]", clusterState)
    masterProxy ! Protocol.Publish(ClusterState.Builder.toBytes(clusterState), system.actorOf(Props(classOf[PublishResponseHandler], ackListener)))
  }

  override def setNodeService(nodeService: NodeService) {
  }

  override def setAllocationService(allocationService: AllocationService) {
  }

  // REST handlers
  //
  //  restController.registerHandler(RestRequest.Method.GET, "/_state", new RestHandler {
  //    override def handleRequest(request: RestRequest, channel: RestChannel) {
  //      channel.sendResponse(new StringRestResponse(RestStatus.OK, cluster.state.toString))
  //    }
  //  })

}

class PublishResponseHandler(ackListener: AckListener) extends Actor {
  override def receive = {
    case Protocol.PublishAck(node, error) =>
      ackListener.onNodeAck(node, error.orNull)
  }
}
