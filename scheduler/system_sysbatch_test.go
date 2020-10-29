package scheduler

import (
	"testing"

	"github.com/hashicorp/nomad/nomad/structs"
	"github.com/stretchr/testify/require"
)

func TestSysbatch_canHandle(t *testing.T) {
	s := SystemScheduler{sysbatch: true}
	t.Run("sysbatch register", func(t *testing.T) {
		require.True(t, s.canHandle(structs.EvalTriggerJobRegister))
	})
	t.Run("sysbatch scheduled", func(t *testing.T) {
		require.False(t, s.canHandle(structs.EvalTriggerScheduled))
	})
	t.Run("sysbatch periodic", func(t *testing.T) {
		require.True(t, s.canHandle(structs.EvalTriggerPeriodicJob))
	})
}

//func TestSysbatch_JobRegister(t *testing.T) {
//	h := NewHarness(t)
//
//	_ = h
//}
