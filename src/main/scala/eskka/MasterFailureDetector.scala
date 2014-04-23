package eskka

import scala.collection.immutable.SortedSet
import scala.concurrent.duration.FiniteDuration

import akka.actor.{ Actor, ActorLogging, Address }
import akka.cluster.{ Cluster, ClusterEvent, Member }
import akka.cluster.ClusterEvent.CurrentClusterState

class MasterFailureDetector(revaluateAfter: FiniteDuration) extends Actor with ActorLogging {

  import eskka.MasterFailureDetector._

  import context.dispatcher

  private[this] val cluster = Cluster(context.system)

  override def preStart() {
    cluster.subscribe(self, ClusterEvent.InitialStateAsEvents, classOf[ClusterEvent.UnreachableMember])
  }

  override def receive = {

    case ClusterEvent.UnreachableMember(m) =>
      scheduleReval(m, "first observation")

    case Revaluate(m) =>
      val cs = cluster.state
      if (cs.members.contains(m)) {
        if (cs.unreachable.contains(m)) {
          if (seenByQuorumOfSeedNodes(cluster.settings.SeedNodes, cs)) {
            if (isMaster(cs, m.address)) {
              log.info("Unreachable member [{}] is master, and this has has been observed by quorum of seed nodes -- downing!", m)
              cluster.down(m.address)
            } else {
              scheduleReval(m, "not master")
            }
          } else {
            scheduleReval(m, "not been observed by a quorum of seed nodes, we may be on a minority partition")
          }
        } else {
          log.info("Member [{}] is reachable now", m)
        }
      } else {
        log.info("Unreachable member [{}] is no longer part of the cluster", m)
      }

  }

  private def scheduleReval(m: Member, reason: String) {
    log.info("Scheduling revaluation of unreachable member [{}] after [{}] -- reason [{}]", m, revaluateAfter, reason)
    context.system.scheduler.scheduleOnce(revaluateAfter, self, Revaluate(m))
  }

  override def postStop() {
    cluster.unsubscribe(self)
  }

}

object MasterFailureDetector {

  private val MemberAgeOrdering = Ordering.fromLessThan[Member](_.isOlderThan(_))

  private case class Revaluate(member: Member)

  private def masterEligibleMembersByAge(cs: CurrentClusterState) =
    SortedSet(cs.members.filter(_.hasRole(MasterRole)).toSeq: _*)(MemberAgeOrdering)

  private def isMaster(cs: CurrentClusterState, address: Address) =
    masterEligibleMembersByAge(cs).headOption.exists(_.address == address)

  private def seenByQuorumOfSeedNodes(seedNodes: Seq[Address], cs: CurrentClusterState) =
    seedNodes.count(cs.seenBy.contains) >= quorumRequirement(seedNodes.size)

}
