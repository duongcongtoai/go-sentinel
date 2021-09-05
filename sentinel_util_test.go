package sentinel

import (
	"go.uber.org/zap/zaptest/observer"
)

func (suite *testSuite) handleLogEventSentinelVotedFor(instanceIdx int, log observer.LoggedEntry) {
	ctxMap := log.ContextMap()

	votedFor := ctxMap["voted_for"].(string)
	term := int(ctxMap["epoch"].(int64))
	currentInstanceID := suite.mapIdxtoRunID[instanceIdx]

	suite.mu.Lock()
	defer suite.mu.Unlock()
	termInfo := suite.termsVote[term][instanceIdx]
	if termInfo.selfVote != "" {
		if termInfo.selfVote != votedFor {
			suite.t.Fatalf("instance %s voted for multiple instances (%s and %s) in the same term %d",
				currentInstanceID, termInfo.selfVote, votedFor, term)
		}
	}
	termInfo.selfVote = votedFor
	if termInfo.neighborVotes == nil {
		termInfo.neighborVotes = map[string]string{}
	}
	suite.termsVote[term][instanceIdx] = termInfo
}

func (suite *testSuite) handleLogEventBecameTermLeader(instanceIdx int, log observer.LoggedEntry) {
	panic("unimplemented")

}

func (suite *testSuite) handleLogEventNeighborVotedFor(instanceIdx int, log observer.LoggedEntry) {
	ctxMap := log.ContextMap()
	term := int(ctxMap["epoch"].(int64))
	neighborID := ctxMap["neighbor_id"].(string)
	votedFor := ctxMap["voted_for"].(string)

	suite.mu.Lock()
	defer suite.mu.Unlock()
	termInfo := suite.termsVote[term][instanceIdx]

	if termInfo.neighborVotes == nil {
		termInfo.neighborVotes = map[string]string{}
	}
	previousRecordedVote := termInfo.neighborVotes[neighborID]

	// already record this neighbor vote before, check if it is consistent
	if previousRecordedVote != "" {
		if previousRecordedVote != votedFor {
			suite.t.Fatalf("neighbor %s is recorded to voted for different leaders (%s and %s) in the same term %d",
				neighborID, previousRecordedVote, votedFor, term,
			)
		}
	}
	termInfo.neighborVotes[neighborID] = votedFor
	suite.termsVote[term][instanceIdx] = termInfo
}
