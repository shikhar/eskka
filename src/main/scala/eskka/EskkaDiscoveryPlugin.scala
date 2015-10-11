package eskka

import org.elasticsearch.common.settings.Settings
import org.elasticsearch.discovery.DiscoveryModule
import org.elasticsearch.plugins.Plugin

class EskkaDiscoveryPlugin(settings: Settings) extends Plugin {

  override def name = "eskka"

  override def description = "Discovery plugin built on top of Akka Cluster"

  def onModule(discoModule: DiscoveryModule): Unit = {
    discoModule.addDiscoveryType("eskka", classOf[EskkaDiscovery])
  }

}
