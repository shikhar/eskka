package eskka

import scala.concurrent.Promise

import org.elasticsearch.cluster.{ClusterService, ClusterState, ProcessedClusterStateUpdateTask}
import org.elasticsearch.common.Priority

object SubmitClusterStateUpdate {

  def apply(clusterService: ClusterService, source: String, update: ClusterState => ClusterState) = {
    val p = Promise[(ClusterState, ClusterState)]()
    clusterService.submitStateUpdateTask(source, Priority.URGENT, new ProcessedClusterStateUpdateTask {

      override def execute(currentState: ClusterState): ClusterState = update(currentState)

      override def clusterStateProcessed(source: String, oldState: ClusterState, newState: ClusterState) {
        p.success(oldState -> newState)
      }

      override def onFailure(source: String, t: Throwable) {
        p.failure(t)
      }

    })
    p.future
  }

}
