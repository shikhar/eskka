package eskka

import akka.actor.Address
import akka.cluster.MemberStatus
import akka.cluster.ClusterEvent.CurrentClusterState

case class VotingMembers(addresses: Set[Address]) {

  val quorumSize = (addresses.size / 2) + 1

  def quorumAvailable(cs: CurrentClusterState) = addresses.count(cs.members.collect {
    // only take into account Up and reachable members
    case m if m.status == MemberStatus.Up && !cs.unreachable(m) => m.address
  }) >= quorumSize

}
