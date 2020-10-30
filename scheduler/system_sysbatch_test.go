package scheduler

import (
	"testing"

	"github.com/hashicorp/go-memdb"
	"github.com/hashicorp/nomad/helper/uuid"
	"github.com/hashicorp/nomad/nomad/mock"
	"github.com/hashicorp/nomad/nomad/structs"
	"github.com/stretchr/testify/require"
)

func TestSysBatch_JobRegister(t *testing.T) {
	h := NewHarness(t)

	// Create some nodes
	for i := 0; i < 10; i++ {
		node := mock.Node()
		require.NoError(t, h.State.UpsertNode(structs.MsgTypeTestSetup, h.NextIndex(), node))
	}

	// Create a job
	job := mock.SystemBatchJob()
	require.NoError(t, h.State.UpsertJob(structs.MsgTypeTestSetup, h.NextIndex(), job))

	// Create a mock evaluation to deregister the job
	eval := &structs.Evaluation{
		Namespace:   structs.DefaultNamespace,
		ID:          uuid.Generate(),
		Priority:    job.Priority,
		TriggeredBy: structs.EvalTriggerJobRegister,
		JobID:       job.ID,
		Status:      structs.EvalStatusPending,
	}
	require.NoError(t, h.State.UpsertEvals(structs.MsgTypeTestSetup, h.NextIndex(), []*structs.Evaluation{eval}))

	// Process the evaluation
	err := h.Process(NewSysBatchScheduler, eval)
	require.NoError(t, err)

	// Ensure a single plan
	require.Len(t, h.Plans, 1)
	plan := h.Plans[0]

	// Ensure the plan does not have annotations
	require.Nil(t, plan.Annotations, "expected no annotations")

	// Ensure the plan allocated
	var planned []*structs.Allocation
	for _, allocList := range plan.NodeAllocation {
		planned = append(planned, allocList...)
	}
	require.Len(t, planned, 10)

	// Lookup the allocations by JobID
	ws := memdb.NewWatchSet()
	out, err := h.State.AllocsByJob(ws, job.Namespace, job.ID, false)
	require.NoError(t, err)

	// Ensure all allocations placed
	require.Len(t, out, 10)

	// Check the available nodes
	count, ok := out[0].Metrics.NodesAvailable["dc1"]
	require.True(t, ok)
	require.Equal(t, 10, count, "bad metrics %#v:", out[0].Metrics)

	// Ensure no allocations are queued
	queued := h.Evals[0].QueuedAllocations["my-sysbatch"]
	require.Equal(t, 0, queued, "unexpected queued allocations")

	h.AssertEvalStatus(t, structs.EvalStatusComplete)
}

func TestSysBatch_JobRegister_AddNode_Running(t *testing.T) {
	h := NewHarness(t)

	// Create some nodes
	var nodes []*structs.Node
	for i := 0; i < 10; i++ {
		node := mock.Node()
		nodes = append(nodes, node)
		require.NoError(t, h.State.UpsertNode(structs.MsgTypeTestSetup, h.NextIndex(), node))
	}

	// Generate a fake sysbatch job with allocations
	job := mock.SystemBatchJob()
	require.NoError(t, h.State.UpsertJob(structs.MsgTypeTestSetup, h.NextIndex(), job))

	var allocs []*structs.Allocation
	for _, node := range nodes {
		alloc := mock.SysBatchAlloc()
		alloc.Job = job
		alloc.JobID = job.ID
		alloc.NodeID = node.ID
		alloc.Name = "my-sysbatch.pinger[0]"
		alloc.ClientStatus = structs.AllocClientStatusRunning
		allocs = append(allocs, alloc)
	}
	require.NoError(t, h.State.UpsertAllocs(structs.MsgTypeTestSetup, h.NextIndex(), allocs))

	// Add a new node.
	node := mock.Node()
	require.NoError(t, h.State.UpsertNode(structs.MsgTypeTestSetup, h.NextIndex(), node))

	// Create a mock evaluation to deal with the node update
	eval := &structs.Evaluation{
		Namespace:   structs.DefaultNamespace,
		ID:          uuid.Generate(),
		Priority:    50,
		TriggeredBy: structs.EvalTriggerNodeUpdate,
		JobID:       job.ID,
		Status:      structs.EvalStatusPending,
	}
	require.NoError(t, h.State.UpsertEvals(structs.MsgTypeTestSetup, h.NextIndex(), []*structs.Evaluation{eval}))

	// Process the evaluation
	err := h.Process(NewSysBatchScheduler, eval)
	require.NoError(t, err)

	// Ensure a single plan
	require.Len(t, h.Plans, 1)
	plan := h.Plans[0]

	// Ensure the plan had no node updates
	var update []*structs.Allocation
	for _, updateList := range plan.NodeUpdate {
		update = append(update, updateList...)
	}
	require.Empty(t, update)

	// Ensure the plan allocated on the new node
	var planned []*structs.Allocation
	for _, allocList := range plan.NodeAllocation {
		planned = append(planned, allocList...)
	}
	require.Len(t, planned, 1)

	// Ensure it allocated on the right node
	_, ok := plan.NodeAllocation[node.ID]
	require.True(t, ok, "allocated on wrong node: %#v", plan)

	// Lookup the allocations by JobID
	ws := memdb.NewWatchSet()
	out, err := h.State.AllocsByJob(ws, job.Namespace, job.ID, false)
	require.NoError(t, err)

	// Ensure all allocations placed
	out, _ = structs.FilterTerminalAllocs(out)
	require.Len(t, out, 11)

	h.AssertEvalStatus(t, structs.EvalStatusComplete)
}

func TestSysBatch_JobRegister_AddNode_Dead(t *testing.T) {
	h := NewHarness(t)

	// Create some nodes
	var nodes []*structs.Node
	for i := 0; i < 10; i++ {
		node := mock.Node()
		nodes = append(nodes, node)
		require.NoError(t, h.State.UpsertNode(structs.MsgTypeTestSetup, h.NextIndex(), node))
	}

	// Generate a dead sysbatch job with complete allocations
	job := mock.SystemBatchJob()
	job.Status = structs.JobStatusDead // job is dead but not stopped
	require.NoError(t, h.State.UpsertJob(structs.MsgTypeTestSetup, h.NextIndex(), job))

	var allocs []*structs.Allocation
	for _, node := range nodes {
		alloc := mock.SysBatchAlloc()
		alloc.Job = job
		alloc.JobID = job.ID
		alloc.NodeID = node.ID
		alloc.Name = "my-sysbatch.pinger[0]"
		alloc.ClientStatus = structs.AllocClientStatusComplete
		allocs = append(allocs, alloc)
	}
	require.NoError(t, h.State.UpsertAllocs(structs.MsgTypeTestSetup, h.NextIndex(), allocs))

	// Add a new node.
	node := mock.Node()
	require.NoError(t, h.State.UpsertNode(structs.MsgTypeTestSetup, h.NextIndex(), node))

	// Create a mock evaluation to deal with the node update
	eval := &structs.Evaluation{
		Namespace:   structs.DefaultNamespace,
		ID:          uuid.Generate(),
		Priority:    50,
		TriggeredBy: structs.EvalTriggerNodeUpdate,
		JobID:       job.ID,
		Status:      structs.EvalStatusPending,
	}
	require.NoError(t, h.State.UpsertEvals(structs.MsgTypeTestSetup, h.NextIndex(), []*structs.Evaluation{eval}))

	// Process the evaluation
	err := h.Process(NewSysBatchScheduler, eval)
	require.NoError(t, err)

	// Ensure a single plan
	require.Len(t, h.Plans, 1)
	plan := h.Plans[0]

	// Ensure the plan has no node update
	var update []*structs.Allocation
	for _, updateList := range plan.NodeUpdate {
		update = append(update, updateList...)
	}
	require.Len(t, update, 0)

	// Ensure the plan allocates on the new node
	var planned []*structs.Allocation
	for _, allocList := range plan.NodeAllocation {
		planned = append(planned, allocList...)
	}
	require.Len(t, planned, 1)

	// Ensure it allocated on the right node
	_, ok := plan.NodeAllocation[node.ID]
	require.True(t, ok, "allocated on wrong node: %#v", plan)

	// Lookup the allocations by JobID
	ws := memdb.NewWatchSet()
	out, err := h.State.AllocsByJob(ws, job.Namespace, job.ID, false)
	require.NoError(t, err)

	// Ensure 1 non-terminal allocation
	live, _ := structs.FilterTerminalAllocs(out)
	require.Len(t, live, 1)

	h.AssertEvalStatus(t, structs.EvalStatusComplete)
}

func TestSysBatch_JobModify(t *testing.T) {
	h := NewHarness(t)

	// Create some nodes
	nodes := make([]*structs.Node, 0, 10)
	for i := 0; i < 10; i++ {
		node := mock.Node()
		nodes = append(nodes, node)
		require.NoError(t, h.State.UpsertNode(structs.MsgTypeTestSetup, h.NextIndex(), node))
	}

	// Generate a fake job with allocations
	job := mock.SystemBatchJob()
	require.NoError(t, h.State.UpsertJob(structs.MsgTypeTestSetup, h.NextIndex(), job))

	var allocs []*structs.Allocation
	for _, node := range nodes {
		alloc := mock.SysBatchAlloc()
		alloc.Job = job
		alloc.JobID = job.ID
		alloc.NodeID = node.ID
		alloc.Name = "my-sysbatch.pinger[0]"
		alloc.ClientStatus = structs.AllocClientStatusPending
		allocs = append(allocs, alloc)
	}
	require.NoError(t, h.State.UpsertAllocs(structs.MsgTypeTestSetup, h.NextIndex(), allocs))

	// Add a few terminal status allocations, these should be reinstated
	var terminal []*structs.Allocation
	for i := 0; i < 5; i++ {
		alloc := mock.SysBatchAlloc()
		alloc.Job = job
		alloc.JobID = job.ID
		alloc.NodeID = nodes[i].ID
		alloc.Name = "my-sysbatch.pinger[0]"
		alloc.ClientStatus = structs.AllocClientStatusComplete
		terminal = append(terminal, alloc)
	}
	require.NoError(t, h.State.UpsertAllocs(structs.MsgTypeTestSetup, h.NextIndex(), terminal))

	// Update the job
	job2 := mock.SystemBatchJob()
	job2.ID = job.ID

	// Update the task, such that it cannot be done in-place
	job2.TaskGroups[0].Tasks[0].Config["command"] = "/bin/other"
	require.NoError(t, h.State.UpsertJob(structs.MsgTypeTestSetup, h.NextIndex(), job2))

	// Create a mock evaluation to deal with drain
	eval := &structs.Evaluation{
		Namespace:   structs.DefaultNamespace,
		ID:          uuid.Generate(),
		Priority:    50,
		TriggeredBy: structs.EvalTriggerJobRegister,
		JobID:       job.ID,
		Status:      structs.EvalStatusPending,
	}
	require.NoError(t, h.State.UpsertEvals(structs.MsgTypeTestSetup, h.NextIndex(), []*structs.Evaluation{eval}))

	// Process the evaluation
	err := h.Process(NewSysBatchScheduler, eval)
	require.NoError(t, err)

	// Ensure a single plan
	require.Len(t, h.Plans, 1)
	plan := h.Plans[0]

	// Ensure the plan evicted all allocs
	var update []*structs.Allocation
	for _, updateList := range plan.NodeUpdate {
		update = append(update, updateList...)
	}
	require.Equal(t, len(allocs), len(update))

	// Ensure the plan allocated
	var planned []*structs.Allocation
	for _, allocList := range plan.NodeAllocation {
		planned = append(planned, allocList...)
	}
	require.Len(t, planned, 10)

	// Lookup the allocations by JobID
	ws := memdb.NewWatchSet()
	out, err := h.State.AllocsByJob(ws, job.Namespace, job.ID, false)
	require.NoError(t, err)

	// Ensure all allocations placed
	out, _ = structs.FilterTerminalAllocs(out)
	require.Len(t, out, 10)

	h.AssertEvalStatus(t, structs.EvalStatusComplete)
}

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
