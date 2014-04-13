package eskka

import scala.collection.JavaConversions._

import org.elasticsearch.common.settings.Settings
import org.elasticsearch.plugins.AbstractPlugin

class EskkaDiscoveryPlugin(settings: Settings) extends AbstractPlugin {

  override def name = "eskka-discovery"

  override def description = "Complete replacement of Zen discovery built on top of Akka cluster support"

  override def modules = Seq(classOf[EskkaDiscoveryModule])

}
