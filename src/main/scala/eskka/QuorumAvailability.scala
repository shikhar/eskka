package eskka

sealed trait QuorumAvailability

case object QuorumAvailable extends QuorumAvailability

case object QuorumUnavailable extends QuorumAvailability
