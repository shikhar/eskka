import akka.actor.Address
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.node.DiscoveryNode

package object eskka {

  object Roles {
    val MasterEligible = "master-eligible"
    val Voter = "voter"
  }

  object ActorNames {
    val CSM = "csm"
    val Master = "master"
    val Follower = "follower"
    val Pinger = "pinger"
  }

  case class ClusterStateTransition(source: String, currentState: ClusterState, prevState: ClusterState)

  case class PublishAck(node: DiscoveryNode, error: Option[Throwable])

  case class VotingMembers(addresses: Set[Address]) {
    val quorumSize = (addresses.size / 2) + 1
  }

}
