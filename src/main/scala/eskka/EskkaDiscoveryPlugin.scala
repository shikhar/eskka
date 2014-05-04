package eskka

import org.elasticsearch.common.settings.Settings
import org.elasticsearch.plugins.AbstractPlugin

class EskkaDiscoveryPlugin(settings: Settings) extends AbstractPlugin {

  override def name = "eskka"

  override def description = "Discovery plugin built on top of Akka Cluster"

}
