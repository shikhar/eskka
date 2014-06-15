package eskka

import java.util.concurrent.TimeUnit

import akka.util.Timeout
import org.elasticsearch.Version
import org.elasticsearch.cluster.node.{ DiscoveryNode, DiscoveryNodeService }
import org.elasticsearch.cluster.routing.allocation.AllocationService
import org.elasticsearch.cluster.{ ClusterName, ClusterService, ClusterState }
import org.elasticsearch.common.component.AbstractLifecycleComponent
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.network.NetworkService
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.discovery.Discovery.AckListener
import org.elasticsearch.discovery.{ Discovery, DiscoveryService, DiscoverySettings, InitialStateDiscoveryListener }
import org.elasticsearch.node.service.NodeService
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.Transport

import concurrent.{ Await, TimeoutException }
import scala.collection.mutable
import scala.concurrent.forkjoin.ThreadLocalRandom

object EskkaDiscovery {

  private val StartTimeout = Timeout(30, TimeUnit.SECONDS)
  private val StartTimeoutFudge = 0.5
  private val LeaveTimeout = Timeout(5, TimeUnit.SECONDS)
  private val ShutdownTimeout = Timeout(5, TimeUnit.SECONDS)

  private def fudgedStartTimeout = {
    val timeoutSeconds = StartTimeout.duration.toSeconds
    val fudgeSeconds = (timeoutSeconds * StartTimeoutFudge).asInstanceOf[Long]
    TimeValue.timeValueSeconds(ThreadLocalRandom.current().nextLong(timeoutSeconds - fudgeSeconds, timeoutSeconds + fudgeSeconds))
  }

}

class EskkaDiscovery @Inject() (clusterName: ClusterName,
                                version: Version,
                                settings: Settings,
                                discoverySettings: DiscoverySettings,
                                threadPool: ThreadPool,
                                transport: Transport,
                                networkService: NetworkService,
                                clusterService: ClusterService,
                                discoveryNodeService: DiscoveryNodeService)
  extends AbstractLifecycleComponent[Discovery](settings) with Discovery {

  import EskkaDiscovery._

  private lazy val nodeId = DiscoveryService.generateNodeId(settings)

  private val initialStateListeners = mutable.LinkedHashSet[InitialStateDiscoveryListener]()

  @volatile private var eskka: EskkaCluster = null

  override def addListener(listener: InitialStateDiscoveryListener) {
    initialStateListeners += listener
  }

  override def removeListener(listener: InitialStateDiscoveryListener) {
    initialStateListeners -= listener
  }

  override def doStart() {
    pleaseDoStart(initial = true)
  }

  override def doStop() {
    Await.ready(eskka.leave(), LeaveTimeout.duration)
  }

  override def doClose() {
    eskka.shutdown()
    eskka.awaitTermination(ShutdownTimeout.duration)
    eskka = null
  }

  private def restartEskka() {
    synchronized {

      logger.debug("restart - stopping eskka")
      try {
        Await.ready(eskka.leave(), LeaveTimeout.duration)
      } catch {
        case te: TimeoutException =>
      }
      eskka.shutdown()
      try {
        eskka.awaitTermination(ShutdownTimeout.duration)
      } catch {
        case te: TimeoutException =>
      }

      logger.debug("restart - starting eskka")
      pleaseDoStart(initial = false)

    }
  }

  private def pleaseDoStart(initial: Boolean) {
    eskka = makeEskkaCluster(initial)
    val up = eskka.start()
    val timeout = fudgedStartTimeout
    threadPool.schedule(timeout, ThreadPool.Names.GENERIC, new Runnable() {
      override def run() {
        if (!up.get()) {
          logger.warn("timeout of {} expired at eskka startup", timeout)
          restartEskka()
        }
      }
    })
  }

  override lazy val localNode = new DiscoveryNode(
    settings.get("name"),
    nodeId,
    transport.boundAddress().publishAddress(),
    discoveryNodeService.buildAttributes(),
    version)

  override def nodeDescription = clusterName.value + "/" + nodeId

  override def publish(clusterState: ClusterState, ackListener: AckListener) {
    logger.trace("publishing new cluster state [{}]", clusterState)
    eskka.publish(clusterState, ackListener)
  }

  override def setNodeService(nodeService: NodeService) {
  }

  override def setAllocationService(allocationService: AllocationService) {
  }

  private def makeEskkaCluster(initial: Boolean): EskkaCluster = {
    new EskkaCluster(clusterName, version, settings, discoverySettings, networkService, clusterService, localNode,
      if (initial) initialStateListeners.toSeq else Seq(), { () =>
        threadPool.generic().execute(new Runnable {
          override def run() {
            restartEskka()
          }
        })
      })
  }

}
