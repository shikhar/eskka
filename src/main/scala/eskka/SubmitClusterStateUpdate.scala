package eskka

import scala.concurrent.Promise

import org.elasticsearch.cluster.{ ClusterService, ClusterState, ProcessedClusterStateUpdateTask }
import org.elasticsearch.common.Priority

object SubmitClusterStateUpdate {

  case class Transition(source: String, currentState: ClusterState, prevState: ClusterState)

  def apply(clusterService: ClusterService,
            source: String,
            update: ClusterState => ClusterState) = {
    val promise = Promise[Transition]()
    clusterService.submitStateUpdateTask(source, Priority.URGENT, new ProcessedClusterStateUpdateTask {

      override def execute(currentState: ClusterState): ClusterState = update(currentState)

      override def clusterStateProcessed(source: String, oldState: ClusterState, newState: ClusterState) {
        promise.success(Transition(source, newState, oldState))
      }

      override def onFailure(source: String, t: Throwable) {
        promise.failure(t)
      }

    })
    promise.future
  }

}
