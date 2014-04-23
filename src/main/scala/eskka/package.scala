package object eskka {

  val MasterRole = "master-eligible"

  def quorumRequirement(n: Int) = math.ceil((n + 1.0) / 2.0).toInt

  object ActorNames {
    val CSM = "csm"
    val Master = "master"
    val Follower = "follower"
  }

}
