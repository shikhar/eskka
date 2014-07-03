package eskka

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import akka.actor._
import akka.cluster.{ Cluster, ClusterEvent }
import akka.pattern.ask
import akka.util.Timeout
import com.google.common.collect.ImmutableList
import com.typesafe.config.ConfigFactory
import org.elasticsearch.Version
import org.elasticsearch.cluster.node.DiscoveryNode
import org.elasticsearch.cluster.{ ClusterName, ClusterService, ClusterState }
import org.elasticsearch.common.logging.Loggers
import org.elasticsearch.common.network.NetworkService
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.discovery.Discovery.AckListener
import org.elasticsearch.discovery.{ DiscoverySettings, InitialStateDiscoveryListener }

import scala.collection.JavaConversions._
import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.util.{ Failure, Success }

object EskkaCluster {
  private val DefaultPort = 9400
  private val PublishTimeoutHard = Timeout(60, TimeUnit.SECONDS)
}

class EskkaCluster(clusterName: ClusterName,
                   version: Version,
                   settings: Settings,
                   discoverySettings: DiscoverySettings,
                   networkService: NetworkService,
                   clusterService: ClusterService,
                   localNode: DiscoveryNode,
                   initialStateListeners: Seq[InitialStateDiscoveryListener],
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
      } else
        None

      val csm = if (cluster.selfRoles.contains(Roles.MasterEligible)) {
        Some(system.actorOf(singleton.ClusterSingletonManager.props(
          singletonProps = Master.props(localNode, votingMembers, version, clusterService),
          singletonName = ActorNames.Master,
          terminationMessage = PoisonPill,
          role = Some(Roles.MasterEligible)), name = ActorNames.CSM))
      } else
        None

      val killSeq = Seq(pinger, follower) ++ Seq(partitionMonitor, csm).flatten
      system.actorOf(QuorumLossAbdicator.props(localNode, votingMembers, clusterService, killSeq, restartHook), "abdicator")

      if (initialStateListeners.nonEmpty) {
        import scala.concurrent.ExecutionContext.Implicits.global
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

    val nonMasterNodes = clusterState.nodes.size - 1

    val (publishResponseHandler, fullyAckedFuture) =
      if (nonMasterNodes > 0) {
        implicit val timeout = if (publishTimeoutMs > 0) Timeout(publishTimeoutMs, TimeUnit.MILLISECONDS) else PublishTimeoutHard
        val handler = system.actorOf(Props(classOf[PublishResponseHandler], nonMasterNodes, ackListener, timeout))
        (handler, handler ? PublishResponseHandler.SubscribeFullyAcked)
      } else {
        (Actor.noSender, Future.successful(PublishResponseHandler.FullyAcked))
      }

    system.actorSelection(s"/user/${ActorNames.CSM}/${ActorNames.Master}").tell(Master.PublishReq(clusterState), publishResponseHandler)

    if (publishTimeoutMs > 0) {
      try {
        Await.ready(fullyAckedFuture, Duration(publishTimeoutMs, TimeUnit.MILLISECONDS))
      } catch {
        case e: TimeoutException =>
          logger.warn("timed out waiting for all nodes to acknowledge cluster state version {}", e, clusterState.version.asInstanceOf[Object])
      }
    }
  }

  def leave(): Future[_] = {
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
    p.future
  }

  def shutdown() {
    system.shutdown()
  }

  def awaitTermination(timeout: Timeout) {
    system.awaitTermination(timeout.duration)
  }

  private def makeActorSystem() = {
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

    val seedNodes = eskkaSettings.getAsArray("seed_nodes", Array(bindHost)).map(addr => if (addr.contains(':')) addr else s"$addr:$DefaultPort")
    val seedNodeAddresses = ImmutableList.copyOf(seedNodes.map(hostPort => s"akka.tcp://$name@$hostPort"))

    val roles = Seq(
      if (isMasterNode) Some(Roles.MasterEligible) else None,
      if (seedNodes.contains(s"$bindHost:$bindPort")) Some(Roles.Voter) else None).flatten

    val quorumOfVoters = new Integer((seedNodes.size / 2) + 1)

    val heartbeatInterval =
      Duration(eskkaSettings.getAsTime("heartbeat_interval", TimeValue.timeValueSeconds(1)).millis(), TimeUnit.MILLISECONDS)

    val acceptableHeartbeatPause =
      Duration(eskkaSettings.getAsTime("acceptable_heartbeat_pause", TimeValue.timeValueSeconds(3)).millis(), TimeUnit.MILLISECONDS)

    val eskkaConfig = ConfigFactory.parseMap(Map(
      "akka.remote.netty.tcp.hostname" -> bindHost,
      "akka.remote.netty.tcp.port" -> bindPort,
      "akka.cluster.seed-nodes" -> seedNodeAddresses,
      "akka.cluster.roles" -> ImmutableList.copyOf(roles.iterator),
      s"akka.cluster.role.${Roles.Voter}.min-nr-of-members" -> quorumOfVoters,
      "akka.cluster.failure-detector.heartbeat-interval" -> s"${heartbeatInterval.toMillis} ms",
      "akka.cluster.failure-detector.acceptable-heartbeat-pause" -> s"${acceptableHeartbeatPause.toMillis} ms"))

    logger.debug("creating actor system with eskka config {}", eskkaConfig)

    ActorSystem(name, config = eskkaConfig.withFallback(ConfigFactory.load()))
  }

  private def partitionEvalDelay =
    Duration(settings.getAsTime("discovery.eskka.partition.eval-delay", TimeValue.timeValueSeconds(10)).millis(), TimeUnit.MILLISECONDS)

  private def partitionPingTimeout =
    Duration(settings.getAsTime("discovery.eskka.partition.ping-timeout", TimeValue.timeValueSeconds(2)).millis(), TimeUnit.MILLISECONDS)

  private def determineBindHost(x: String) = {
    if ((x.startsWith("#") && x.endsWith("#")) || (x.startsWith("_") && x.endsWith("_")))
      networkService.resolveBindHostAddress(x).getHostAddress
    else
      x
  }

}
