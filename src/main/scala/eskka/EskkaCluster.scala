package eskka

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import akka.actor._
import akka.cluster.Cluster
import akka.cluster.singleton.{ ClusterSingletonManager, ClusterSingletonManagerSettings }
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.elasticsearch.cluster.node.DiscoveryNode
import org.elasticsearch.cluster.{ ClusterName, ClusterService, ClusterState }
import org.elasticsearch.common.logging.Loggers
import org.elasticsearch.common.network.NetworkService
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.discovery.Discovery.AckListener
import org.elasticsearch.discovery.{ DiscoverySettings, InitialStateDiscoveryListener }
import org.elasticsearch.threadpool.ThreadPool

import scala.collection.JavaConverters._
import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.util.{ Failure, Success }

object EskkaCluster {
  private val DefaultPort = 9400
  private val PublishTimeoutHard = Timeout(60, TimeUnit.SECONDS)
}

class EskkaCluster(clusterName: ClusterName,
                   settings: Settings,
                   discoverySettings: DiscoverySettings,
                   threadPool: ThreadPool,
                   networkService: NetworkService,
                   clusterService: ClusterService,
                   localNode: DiscoveryNode,
                   initialStateListeners: List[InitialStateDiscoveryListener],
                   restartHook: () => Unit) {

  import EskkaCluster._

  private[this] val logger = Loggers.getLogger(getClass, settings)

  private[this] val system = makeActorSystem()
  private[this] val cluster = Cluster(system)

  def start(): AtomicBoolean = {
    if (cluster.settings.SeedNodes.size < 3) {
      logger.warn("Recommended to configure 3 or more seed nodes using `eskka.seed_nodes`")
    }

    val votingMembers = VotingMembers(cluster.settings.SeedNodes.toSet)

    val memberUp = new AtomicBoolean()

    cluster.registerOnMemberUp {

      val pinger = system.actorOf(Pinger.props, ActorNames.Pinger)

      val follower = system.actorOf(Follower.props(localNode, votingMembers, clusterService), ActorNames.Follower)

      val partitionMonitor = if (cluster.selfRoles(Roles.Voter)) {
        Some(system.actorOf(QuorumBasedPartitionMonitor.props(votingMembers, partitionEvalDelay, partitionPingTimeout), "partition-monitor"))
      } else {
        None
      }

      val csm = if (cluster.selfRoles.contains(Roles.MasterEligible)) {
        val csmSettings = new ClusterSingletonManagerSettings(
          singletonName = ActorNames.Master,
          role = Some(Roles.MasterEligible),
          removalMargin = Duration.Zero, // TODO wht
          handOverRetryInterval = Duration(1, TimeUnit.SECONDS) // TODO wht
        )
        Some(system.actorOf(ClusterSingletonManager.props(Master.props(localNode, votingMembers, clusterService), PoisonPill, csmSettings), name = ActorNames.CSM))
      } else
        None

      val killSeq = List(pinger, follower) ++ List(partitionMonitor, csm).flatten
      system.actorOf(QuorumLossAbdicator.props(localNode, votingMembers, discoverySettings, clusterService, killSeq, restartHook), "abdicator")

      if (initialStateListeners.nonEmpty) {
        import system.dispatcher
        implicit val timeout = Timeout(discoverySettings.getPublishTimeout.getMillis, TimeUnit.MILLISECONDS)
        (follower ? Follower.CheckInitSub) onComplete {
          case Success(info) =>
            initialStateListeners.foreach(_.initialStateProcessed())
            logger.debug("Initial state processed -- {}", info.asInstanceOf[Object])
          case Failure(e) =>
            logger.error("Initial state processing failed!", e)
        }
      }

      memberUp.set(true)
    }

    memberUp
  }

  def publish(clusterState: ClusterState, ackListener: AckListener) {
    val publishTimeoutMs = discoverySettings.getPublishTimeout.millis

    val nonMasterNodes = (Set() ++ clusterState.nodes.asScala) - localNode

    val publishResponseHandler =
      if (nonMasterNodes.nonEmpty) {
        val timeout = if (publishTimeoutMs > 0) Timeout(publishTimeoutMs, TimeUnit.MILLISECONDS) else PublishTimeoutHard
        system.actorOf(PublishResponseHandler.props(nonMasterNodes, threadPool, ackListener, timeout))
      } else {
        Actor.noSender
      }

    system.actorSelection(s"/user/${ActorNames.CSM}/${ActorNames.Master}").tell(Master.PublishReq(clusterState), publishResponseHandler)
  }

  def leave(context: String): Future[_] = {
    logger.info("Leaving the cluster [{}]", context)
    val p = Promise[Any]()
    cluster.registerOnMemberRemoved(p.success(Nil))
    cluster.leave(cluster.selfAddress)
    p.future
  }

  def shutdown(context: String): Future[_] = {
    logger.info("Terminating actor system [{}]", context)
    system.terminate()
  }

  private def makeActorSystem(): ActorSystem = {
    val name = clusterName.value
    val nodeSettings = settings.getByPrefix("node.")
    val isClientNode = nodeSettings.getAsBoolean("client", false)
    val isMasterNode = nodeSettings.getAsBoolean("master", !isClientNode)

    val eskkaSettings = settings.getByPrefix("discovery.eskka.")

    val bindHost =
      determineBindHost(
        eskkaSettings.get("host",
          settings.get("transport.bind_host",
            settings.get("transport.host",
              settings.get("network.bind_host",
                settings.get("network.host", "_local_"))))))

    val bindPort = eskkaSettings.getAsInt("port", if (isClientNode) 0 else DefaultPort)

    val seedNodes = eskkaSettings.getAsArray("seed_nodes", Array(bindHost)).map(addr => if (addr.contains(':')) addr else s"$addr:$DefaultPort").toList
    val seedNodeAddresses = seedNodes.map(hostPort => s"akka.tcp://$name@$hostPort")

    val roles = List(
      if (isMasterNode) Some(Roles.MasterEligible) else None,
      if (seedNodes.contains(s"$bindHost:$bindPort")) Some(Roles.Voter) else None
    ).flatten

    val quorumOfVoters = (seedNodes.size / 2) + 1

    val heartbeatInterval =
      Duration(eskkaSettings.getAsTime("heartbeat_interval", TimeValue.timeValueSeconds(1)).millis(), TimeUnit.MILLISECONDS) // TODO wht

    val acceptableHeartbeatPause =
      Duration(eskkaSettings.getAsTime("acceptable_heartbeat_pause", TimeValue.timeValueSeconds(3)).millis(), TimeUnit.MILLISECONDS) // TODO wht

    val eskkaConfig = ConfigFactory.parseMap(Map(
      "akka.daemonic" -> "on",
      "akka.remote.netty.tcp.hostname" -> bindHost,
      "akka.remote.netty.tcp.port" -> bindPort,
      "akka.cluster.seed-nodes" -> seedNodeAddresses.asJava,
      "akka.cluster.roles" -> roles.asJava,
      s"akka.cluster.role.${Roles.Voter}.min-nr-of-members" -> new Integer(quorumOfVoters),
      "akka.cluster.failure-detector.heartbeat-interval" -> s"${heartbeatInterval.toMillis} ms",
      "akka.cluster.failure-detector.acceptable-heartbeat-pause" -> s"${acceptableHeartbeatPause.toMillis} ms").asJava)

    logger.debug("creating actor system with eskka config {}", eskkaConfig)

    val classLoader = getClass.getClassLoader
    ActorSystem(name, classLoader = Some(classLoader), config = Some(eskkaConfig.withFallback(ConfigFactory.load(classLoader))))
  }

  private def partitionEvalDelay =
    Duration(settings.getAsTime("discovery.eskka.partition.eval-delay", TimeValue.timeValueSeconds(10)).millis(), TimeUnit.MILLISECONDS) // TODO wht

  private def partitionPingTimeout =
    Duration(settings.getAsTime("discovery.eskka.partition.ping-timeout", TimeValue.timeValueSeconds(2)).millis(), TimeUnit.MILLISECONDS) // TODO wht

  private def determineBindHost(x: String): String = {
    if ((x.startsWith("#") && x.endsWith("#")) || (x.startsWith("_") && x.endsWith("_"))) // TODO wht
      networkService.resolveBindHostAddress(x)(0).getHostAddress  // TODO wht
    else
      x
  }

}
