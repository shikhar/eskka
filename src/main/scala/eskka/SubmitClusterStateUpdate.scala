package eskka

import org.elasticsearch.cluster.{ ClusterService, ClusterState, ProcessedClusterStateUpdateTask }
import org.elasticsearch.common.Priority

import scala.concurrent.{ Future, Promise }

object SubmitClusterStateUpdate {

  def apply(clusterService: ClusterService,
            source: String,
            priority: Priority,
            runOnlyOnMaster: Boolean,
            update: ClusterState => ClusterState): Future[ClusterStateTransition] = {
    val promise = Promise[ClusterStateTransition]()
    clusterService.submitStateUpdateTask(source, priority, new ProcessedClusterStateUpdateTask {

      override def runOnlyOnMaster = false

      override def execute(currentState: ClusterState): ClusterState = update(currentState)

      override def clusterStateProcessed(source: String, oldState: ClusterState, newState: ClusterState) {
        promise.success(ClusterStateTransition(source, newState, oldState))
      }

      override def onFailure(source: String, t: Throwable) {
        promise.failure(t)
      }

    })
    promise.future
  }

}
