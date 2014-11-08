import akka.actor.Address
import akka.util.ByteString
import org.elasticsearch.cluster.ClusterState

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

  case class PublishAck(node: ByteString, error: Option[Throwable])

  case class VotingMembers(addresses: Set[Address]) {
    val quorumSize = (addresses.size / 2) + 1
  }

}
