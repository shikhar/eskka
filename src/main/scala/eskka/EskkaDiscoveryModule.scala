package eskka

import org.elasticsearch.common.inject.AbstractModule
import org.elasticsearch.discovery.Discovery

class EskkaDiscoveryModule extends AbstractModule {

  override def configure() {
    bind(classOf[Discovery]).to(classOf[EskkaDiscovery]).asEagerSingleton()
  }

}
