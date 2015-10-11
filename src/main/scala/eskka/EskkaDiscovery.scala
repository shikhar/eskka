package eskka

import java.util.concurrent.TimeUnit

import akka.util.Timeout
import org.elasticsearch.Version
import org.elasticsearch.cluster.node.{DiscoveryNode, DiscoveryNodeService}
import org.elasticsearch.cluster.routing.RoutingService
import org.elasticsearch.cluster.{ClusterChangedEvent, ClusterName, ClusterService}
import org.elasticsearch.common.component.AbstractLifecycleComponent
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.network.NetworkService
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.discovery.Discovery.AckListener
import org.elasticsearch.discovery.{Discovery, DiscoveryService, DiscoverySettings, InitialStateDiscoveryListener}
import org.elasticsearch.node.service.NodeService
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.Transport

import scala.collection.mutable
import scala.concurrent.forkjoin.ThreadLocalRandom
import scala.concurrent.{Await, TimeoutException}
import scala.util.control.Exception

object EskkaDiscovery {

  private val StartTimeout = Timeout(60, TimeUnit.SECONDS)
  private val StartTimeoutFudge = 0.5
  private val LeaveTimeout = Timeout(4, TimeUnit.SECONDS)
  private val ShutdownTimeout = Timeout(1, TimeUnit.SECONDS)

  private def fudgedStartTimeout = {
    val timeoutSeconds = StartTimeout.duration.toSeconds
    val fudgeSeconds = (timeoutSeconds * StartTimeoutFudge).asInstanceOf[Long]
    TimeValue.timeValueSeconds(ThreadLocalRandom.current().nextLong(timeoutSeconds - fudgeSeconds, timeoutSeconds + fudgeSeconds))
  }

  private val TimeoutExceptionIgnored = Exception.ignoring(classOf[TimeoutException])

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

  @volatile private var moduleStopped = false

  @volatile private var eskka: Option[EskkaCluster] = None

  override def addListener(listener: InitialStateDiscoveryListener) {
    initialStateListeners += listener
  }

  override def removeListener(listener: InitialStateDiscoveryListener) {
    initialStateListeners -= listener
  }

  override def doStart() {
    initEskka(initial = true, "module-start")
  }

  override def doStop() {
    moduleStopped = true

    synchronized {
      destroyEskka("module-stop")
    }
  }

  override def doClose() {
  }

  private def restartEskka(context: String) {
    synchronized {
      if (!moduleStopped) {
        destroyEskka(context)
      }
      if (!moduleStopped) {
        initEskka(initial = false, context)
      }
    }
  }

  private def initEskka(initial: Boolean, context: String) {
    require(eskka.isEmpty)
    logger.info("starting eskka [{}]", context)
    val e = makeEskkaCluster(initial)
    eskka = Some(e)
    val up = e.start()
    val timeout = fudgedStartTimeout
    threadPool.schedule(timeout, ThreadPool.Names.GENERIC, new Runnable() {
      override def run() {
        if (!up.get()) {
          logger.warn("timeout of {} expired at eskka startup", timeout)
          restartEskka("startup-timeout")
        }
      }
    })
  }

  private def destroyEskka(context: String) {
    for (e <- eskka) {
      TimeoutExceptionIgnored(Await.ready(e.leave(context), LeaveTimeout.duration))
      TimeoutExceptionIgnored(Await.ready(e.shutdown(context), ShutdownTimeout.duration))
    }
    eskka = None
  }

  override lazy val localNode = new DiscoveryNode(
    settings.get("name"),
    nodeId,
    transport.boundAddress().publishAddress(),
    discoveryNodeService.buildAttributes(),
    version)

  override def nodeDescription = clusterName.value + "/" + nodeId

  override def publish(changeEvent: ClusterChangedEvent, ackListener: AckListener) {
    logger.trace("publishing new cluster state [{}]", changeEvent.state())
    eskka.getOrElse(throw new IllegalStateException("eskka is not available")).publish(changeEvent.state(), ackListener)
  }

  override def setNodeService(nodeService: NodeService) {
  }

  override def setRoutingService(routingService: RoutingService) {
  }

  private def makeEskkaCluster(initial: Boolean): EskkaCluster = {
    new EskkaCluster(clusterName, settings, discoverySettings, threadPool, networkService, clusterService, localNode,
      if (initial) initialStateListeners.toList else Nil, { () =>
        threadPool.generic().execute(new Runnable {
          override def run() {
            restartEskka("restart")
          }
        })
      })
  }

}
