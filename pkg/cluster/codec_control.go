package cluster

import (
	"encoding/binary"
	"time"

	controllermeta "github.com/WuKongIM/WuKongIM/pkg/controller/meta"
	slotcontroller "github.com/WuKongIM/WuKongIM/pkg/controller/plane"
	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
)

const (
	rpcServiceController          uint8            = 14
	controllerRPCShardKey         multiraft.SlotID = multiraft.SlotID(^uint32(0))
	controllerRPCHeartbeat        string           = "heartbeat"
	controllerRPCListAssignments  string           = "list_assignments"
	controllerRPCListNodes        string           = "list_nodes"
	controllerRPCListRuntimeViews string           = "list_runtime_views"
	controllerRPCOperator         string           = "operator"
	controllerRPCGetTask          string           = "get_task"
	controllerRPCForceReconcile   string           = "force_reconcile"
	controllerRPCTaskResult       string           = "task_result"
)

const (
	controllerCodecVersion byte = 1

	controllerRespFlagNotLeader byte = 1 << iota
	controllerRespFlagNotFound
)

const (
	controllerKindUnknown byte = iota
	controllerKindHeartbeat
	controllerKindListAssignments
	controllerKindListNodes
	controllerKindListRuntimeViews
	controllerKindOperator
	controllerKindGetTask
	controllerKindForceReconcile
	controllerKindTaskResult
)

type controllerRPCRequest struct {
	Kind    string
	SlotID  uint32
	Report  *slotcontroller.AgentReport
	Op      *slotcontroller.OperatorRequest
	Advance *controllerTaskAdvance
}

type controllerTaskAdvance struct {
	SlotID  uint32
	Attempt uint32
	Now     time.Time
	Err     string
}

type controllerRPCResponse struct {
	NotLeader    bool
	NotFound     bool
	LeaderID     uint64
	Nodes        []controllermeta.ClusterNode
	Assignments  []controllermeta.SlotAssignment
	RuntimeViews []controllermeta.SlotRuntimeView
	Task         *controllermeta.ReconcileTask
}

func encodeControllerRequest(req controllerRPCRequest) ([]byte, error) {
	kind, err := controllerKindCode(req.Kind)
	if err != nil {
		return nil, err
	}

	slotID := req.SlotID
	if slotID == 0 && req.Advance != nil {
		slotID = req.Advance.SlotID
	}

	payload, err := encodeControllerRequestPayload(req)
	if err != nil {
		return nil, err
	}

	body := make([]byte, 0, 1+1+4+binary.MaxVarintLen64+len(payload))
	body = append(body, controllerCodecVersion, kind)
	body = binary.BigEndian.AppendUint32(body, slotID)
	body = binary.AppendUvarint(body, uint64(len(payload)))
	body = append(body, payload...)
	return body, nil
}

func decodeControllerRequest(body []byte) (controllerRPCRequest, error) {
	if len(body) < 1+1+4 {
		return controllerRPCRequest{}, ErrInvalidConfig
	}
	if body[0] != controllerCodecVersion {
		return controllerRPCRequest{}, ErrInvalidConfig
	}

	kind, err := controllerKindName(body[1])
	if err != nil {
		return controllerRPCRequest{}, err
	}
	slotID := binary.BigEndian.Uint32(body[2:6])
	payload, err := readControllerPayload(body[6:])
	if err != nil {
		return controllerRPCRequest{}, err
	}

	req := controllerRPCRequest{
		Kind:   kind,
		SlotID: slotID,
	}
	if err := decodeControllerRequestPayload(&req, payload); err != nil {
		return controllerRPCRequest{}, err
	}
	return req, nil
}

func encodeControllerResponse(kind string, resp controllerRPCResponse) ([]byte, error) {
	flags := byte(0)
	if resp.NotLeader {
		flags |= controllerRespFlagNotLeader
	}
	if resp.NotFound {
		flags |= controllerRespFlagNotFound
	}

	payload, err := encodeControllerResponsePayload(kind, resp)
	if err != nil {
		return nil, err
	}

	body := make([]byte, 0, 1+1+8+binary.MaxVarintLen64+len(payload))
	body = append(body, controllerCodecVersion, flags)
	body = binary.BigEndian.AppendUint64(body, resp.LeaderID)
	body = binary.AppendUvarint(body, uint64(len(payload)))
	body = append(body, payload...)
	return body, nil
}

func decodeControllerResponse(kind string, body []byte) (controllerRPCResponse, error) {
	if len(body) < 1+1+8 {
		return controllerRPCResponse{}, ErrInvalidConfig
	}
	if body[0] != controllerCodecVersion {
		return controllerRPCResponse{}, ErrInvalidConfig
	}

	payload, err := readControllerPayload(body[10:])
	if err != nil {
		return controllerRPCResponse{}, err
	}

	resp := controllerRPCResponse{
		NotLeader: body[1]&controllerRespFlagNotLeader != 0,
		NotFound:  body[1]&controllerRespFlagNotFound != 0,
		LeaderID:  binary.BigEndian.Uint64(body[2:10]),
	}
	if err := decodeControllerResponsePayload(kind, &resp, payload); err != nil {
		return controllerRPCResponse{}, err
	}
	return resp, nil
}

func encodeControllerRequestPayload(req controllerRPCRequest) ([]byte, error) {
	switch req.Kind {
	case controllerRPCHeartbeat:
		if req.Report == nil {
			return nil, ErrInvalidConfig
		}
		return encodeAgentReport(*req.Report), nil
	case controllerRPCOperator:
		if req.Op == nil {
			return nil, ErrInvalidConfig
		}
		return encodeOperatorRequest(*req.Op), nil
	case controllerRPCTaskResult:
		if req.Advance == nil {
			return nil, ErrInvalidConfig
		}
		return encodeTaskAdvance(*req.Advance), nil
	case controllerRPCListAssignments, controllerRPCListNodes, controllerRPCListRuntimeViews, controllerRPCGetTask, controllerRPCForceReconcile:
		return nil, nil
	default:
		return nil, ErrInvalidConfig
	}
}

func decodeControllerRequestPayload(req *controllerRPCRequest, payload []byte) error {
	switch req.Kind {
	case controllerRPCHeartbeat:
		report, err := decodeAgentReport(payload)
		if err != nil {
			return err
		}
		req.Report = &report
		return nil
	case controllerRPCOperator:
		op, err := decodeOperatorRequest(payload)
		if err != nil {
			return err
		}
		req.Op = &op
		return nil
	case controllerRPCTaskResult:
		advance, err := decodeTaskAdvance(req.SlotID, payload)
		if err != nil {
			return err
		}
		req.Advance = &advance
		return nil
	case controllerRPCListAssignments, controllerRPCListNodes, controllerRPCListRuntimeViews, controllerRPCGetTask, controllerRPCForceReconcile:
		if len(payload) != 0 {
			return ErrInvalidConfig
		}
		return nil
	default:
		return ErrInvalidConfig
	}
}

func encodeControllerResponsePayload(kind string, resp controllerRPCResponse) ([]byte, error) {
	switch kind {
	case controllerRPCHeartbeat, controllerRPCOperator, controllerRPCForceReconcile, controllerRPCTaskResult:
		return nil, nil
	case controllerRPCListAssignments:
		return encodeAssignments(resp.Assignments), nil
	case controllerRPCListNodes:
		return encodeClusterNodes(resp.Nodes), nil
	case controllerRPCListRuntimeViews:
		return encodeRuntimeViews(resp.RuntimeViews), nil
	case controllerRPCGetTask:
		if resp.Task == nil {
			return nil, nil
		}
		return encodeReconcileTask(*resp.Task), nil
	default:
		return nil, ErrInvalidConfig
	}
}

func decodeControllerResponsePayload(kind string, resp *controllerRPCResponse, payload []byte) error {
	switch kind {
	case controllerRPCHeartbeat, controllerRPCOperator, controllerRPCForceReconcile, controllerRPCTaskResult:
		if len(payload) != 0 {
			return ErrInvalidConfig
		}
		return nil
	case controllerRPCListAssignments:
		assignments, err := decodeAssignments(payload)
		if err != nil {
			return err
		}
		resp.Assignments = assignments
		return nil
	case controllerRPCListNodes:
		nodes, err := decodeClusterNodes(payload)
		if err != nil {
			return err
		}
		resp.Nodes = nodes
		return nil
	case controllerRPCListRuntimeViews:
		views, err := decodeRuntimeViews(payload)
		if err != nil {
			return err
		}
		resp.RuntimeViews = views
		return nil
	case controllerRPCGetTask:
		if len(payload) == 0 {
			return nil
		}
		task, err := decodeReconcileTask(payload)
		if err != nil {
			return err
		}
		resp.Task = &task
		return nil
	default:
		return ErrInvalidConfig
	}
}

func controllerKindCode(kind string) (byte, error) {
	switch kind {
	case controllerRPCHeartbeat:
		return controllerKindHeartbeat, nil
	case controllerRPCListAssignments:
		return controllerKindListAssignments, nil
	case controllerRPCListNodes:
		return controllerKindListNodes, nil
	case controllerRPCListRuntimeViews:
		return controllerKindListRuntimeViews, nil
	case controllerRPCOperator:
		return controllerKindOperator, nil
	case controllerRPCGetTask:
		return controllerKindGetTask, nil
	case controllerRPCForceReconcile:
		return controllerKindForceReconcile, nil
	case controllerRPCTaskResult:
		return controllerKindTaskResult, nil
	default:
		return controllerKindUnknown, ErrInvalidConfig
	}
}

func controllerKindName(kind byte) (string, error) {
	switch kind {
	case controllerKindHeartbeat:
		return controllerRPCHeartbeat, nil
	case controllerKindListAssignments:
		return controllerRPCListAssignments, nil
	case controllerKindListNodes:
		return controllerRPCListNodes, nil
	case controllerKindListRuntimeViews:
		return controllerRPCListRuntimeViews, nil
	case controllerKindOperator:
		return controllerRPCOperator, nil
	case controllerKindGetTask:
		return controllerRPCGetTask, nil
	case controllerKindForceReconcile:
		return controllerRPCForceReconcile, nil
	case controllerKindTaskResult:
		return controllerRPCTaskResult, nil
	default:
		return "", ErrInvalidConfig
	}
}

func readControllerPayload(body []byte) ([]byte, error) {
	payloadLen, n := binary.Uvarint(body)
	if n <= 0 {
		return nil, ErrInvalidConfig
	}
	body = body[n:]
	if len(body) != int(payloadLen) {
		return nil, ErrInvalidConfig
	}
	return body, nil
}

func encodeAgentReport(report slotcontroller.AgentReport) []byte {
	body := make([]byte, 0, 64+len(report.Addr))
	body = binary.BigEndian.AppendUint64(body, report.NodeID)
	body = appendString(body, report.Addr)
	body = appendInt64(body, report.ObservedAt.UnixNano())
	body = appendInt64(body, int64(report.CapacityWeight))
	if report.Runtime != nil {
		body = append(body, 1)
		body = appendRuntimeView(body, *report.Runtime)
		return body
	}
	return append(body, 0)
}

func decodeAgentReport(body []byte) (slotcontroller.AgentReport, error) {
	nodeID, rest, err := readUint64(body)
	if err != nil {
		return slotcontroller.AgentReport{}, err
	}
	addr, rest, err := readString(rest)
	if err != nil {
		return slotcontroller.AgentReport{}, err
	}
	observedAtUnix, rest, err := readInt64(rest)
	if err != nil {
		return slotcontroller.AgentReport{}, err
	}
	capacityWeight, rest, err := readInt64(rest)
	if err != nil {
		return slotcontroller.AgentReport{}, err
	}
	if len(rest) < 1 {
		return slotcontroller.AgentReport{}, ErrInvalidConfig
	}

	report := slotcontroller.AgentReport{
		NodeID:         nodeID,
		Addr:           addr,
		ObservedAt:     time.Unix(0, observedAtUnix),
		CapacityWeight: int(capacityWeight),
	}
	if rest[0] == 0 {
		if len(rest) != 1 {
			return slotcontroller.AgentReport{}, ErrInvalidConfig
		}
		return report, nil
	}
	view, remaining, err := consumeRuntimeView(rest[1:])
	if err != nil || len(remaining) != 0 {
		return slotcontroller.AgentReport{}, ErrInvalidConfig
	}
	report.Runtime = &view
	return report, nil
}

func encodeOperatorRequest(op slotcontroller.OperatorRequest) []byte {
	body := make([]byte, 0, 9)
	body = append(body, byte(op.Kind))
	return binary.BigEndian.AppendUint64(body, op.NodeID)
}

func decodeOperatorRequest(body []byte) (slotcontroller.OperatorRequest, error) {
	if len(body) != 9 {
		return slotcontroller.OperatorRequest{}, ErrInvalidConfig
	}
	return slotcontroller.OperatorRequest{
		Kind:   slotcontroller.OperatorKind(body[0]),
		NodeID: binary.BigEndian.Uint64(body[1:9]),
	}, nil
}

func encodeTaskAdvance(advance controllerTaskAdvance) []byte {
	body := make([]byte, 0, 4+8+binary.MaxVarintLen64+len(advance.Err))
	body = binary.BigEndian.AppendUint32(body, advance.Attempt)
	body = appendInt64(body, advance.Now.UnixNano())
	body = appendString(body, advance.Err)
	return body
}

func decodeTaskAdvance(slotID uint32, body []byte) (controllerTaskAdvance, error) {
	attempt, rest, err := readUint32(body)
	if err != nil {
		return controllerTaskAdvance{}, err
	}
	nowUnix, rest, err := readInt64(rest)
	if err != nil {
		return controllerTaskAdvance{}, err
	}
	taskErr, rest, err := readString(rest)
	if err != nil || len(rest) != 0 {
		return controllerTaskAdvance{}, ErrInvalidConfig
	}
	return controllerTaskAdvance{
		SlotID:  slotID,
		Attempt: attempt,
		Now:     time.Unix(0, nowUnix),
		Err:     taskErr,
	}, nil
}

func encodeClusterNodes(nodes []controllermeta.ClusterNode) []byte {
	body := binary.AppendUvarint(make([]byte, 0, len(nodes)*32), uint64(len(nodes)))
	for _, node := range nodes {
		body = appendClusterNode(body, node)
	}
	return body
}

func decodeClusterNodes(body []byte) ([]controllermeta.ClusterNode, error) {
	count, rest, err := readUvarint(body)
	if err != nil {
		return nil, err
	}
	nodes := make([]controllermeta.ClusterNode, 0, count)
	for i := uint64(0); i < count; i++ {
		node, next, err := consumeClusterNode(rest)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
		rest = next
	}
	if len(rest) != 0 {
		return nil, ErrInvalidConfig
	}
	return nodes, nil
}

func appendClusterNode(dst []byte, node controllermeta.ClusterNode) []byte {
	dst = binary.BigEndian.AppendUint64(dst, node.NodeID)
	dst = appendString(dst, node.Addr)
	dst = append(dst, byte(node.Status))
	dst = appendInt64(dst, node.LastHeartbeatAt.UnixNano())
	return appendInt64(dst, int64(node.CapacityWeight))
}

func consumeClusterNode(body []byte) (controllermeta.ClusterNode, []byte, error) {
	nodeID, rest, err := readUint64(body)
	if err != nil {
		return controllermeta.ClusterNode{}, nil, err
	}
	addr, rest, err := readString(rest)
	if err != nil {
		return controllermeta.ClusterNode{}, nil, err
	}
	if len(rest) < 1 {
		return controllermeta.ClusterNode{}, nil, ErrInvalidConfig
	}
	status := controllermeta.NodeStatus(rest[0])
	lastHeartbeatAtUnix, rest, err := readInt64(rest[1:])
	if err != nil {
		return controllermeta.ClusterNode{}, nil, err
	}
	capacityWeight, rest, err := readInt64(rest)
	if err != nil {
		return controllermeta.ClusterNode{}, nil, err
	}
	return controllermeta.ClusterNode{
		NodeID:          nodeID,
		Addr:            addr,
		Status:          status,
		LastHeartbeatAt: time.Unix(0, lastHeartbeatAtUnix),
		CapacityWeight:  int(capacityWeight),
	}, rest, nil
}

func encodeAssignments(assignments []controllermeta.SlotAssignment) []byte {
	body := binary.AppendUvarint(make([]byte, 0, len(assignments)*32), uint64(len(assignments)))
	for _, assignment := range assignments {
		body = appendAssignment(body, assignment)
	}
	return body
}

func decodeAssignments(body []byte) ([]controllermeta.SlotAssignment, error) {
	count, rest, err := readUvarint(body)
	if err != nil {
		return nil, err
	}
	assignments := make([]controllermeta.SlotAssignment, 0, count)
	for i := uint64(0); i < count; i++ {
		assignment, next, err := consumeAssignment(rest)
		if err != nil {
			return nil, err
		}
		assignments = append(assignments, assignment)
		rest = next
	}
	if len(rest) != 0 {
		return nil, ErrInvalidConfig
	}
	return assignments, nil
}

func appendAssignment(dst []byte, assignment controllermeta.SlotAssignment) []byte {
	dst = binary.BigEndian.AppendUint32(dst, assignment.SlotID)
	dst = appendUint64Slice(dst, assignment.DesiredPeers)
	dst = binary.BigEndian.AppendUint64(dst, assignment.ConfigEpoch)
	return binary.BigEndian.AppendUint64(dst, assignment.BalanceVersion)
}

func consumeAssignment(body []byte) (controllermeta.SlotAssignment, []byte, error) {
	slotID, rest, err := readUint32(body)
	if err != nil {
		return controllermeta.SlotAssignment{}, nil, err
	}
	desiredPeers, rest, err := readUint64Slice(rest)
	if err != nil {
		return controllermeta.SlotAssignment{}, nil, err
	}
	configEpoch, rest, err := readUint64(rest)
	if err != nil {
		return controllermeta.SlotAssignment{}, nil, err
	}
	balanceVersion, rest, err := readUint64(rest)
	if err != nil {
		return controllermeta.SlotAssignment{}, nil, err
	}
	return controllermeta.SlotAssignment{
		SlotID:         slotID,
		DesiredPeers:   desiredPeers,
		ConfigEpoch:    configEpoch,
		BalanceVersion: balanceVersion,
	}, rest, nil
}

func encodeRuntimeViews(views []controllermeta.SlotRuntimeView) []byte {
	body := binary.AppendUvarint(make([]byte, 0, len(views)*40), uint64(len(views)))
	for _, view := range views {
		body = appendRuntimeView(body, view)
	}
	return body
}

func decodeRuntimeViews(body []byte) ([]controllermeta.SlotRuntimeView, error) {
	count, rest, err := readUvarint(body)
	if err != nil {
		return nil, err
	}
	views := make([]controllermeta.SlotRuntimeView, 0, count)
	for i := uint64(0); i < count; i++ {
		view, next, err := consumeRuntimeView(rest)
		if err != nil {
			return nil, err
		}
		views = append(views, view)
		rest = next
	}
	if len(rest) != 0 {
		return nil, ErrInvalidConfig
	}
	return views, nil
}

func appendRuntimeView(dst []byte, view controllermeta.SlotRuntimeView) []byte {
	dst = binary.BigEndian.AppendUint32(dst, view.SlotID)
	dst = appendUint64Slice(dst, view.CurrentPeers)
	dst = binary.BigEndian.AppendUint64(dst, view.LeaderID)
	dst = binary.BigEndian.AppendUint32(dst, view.HealthyVoters)
	if view.HasQuorum {
		dst = append(dst, 1)
	} else {
		dst = append(dst, 0)
	}
	dst = binary.BigEndian.AppendUint64(dst, view.ObservedConfigEpoch)
	return appendInt64(dst, view.LastReportAt.UnixNano())
}

func consumeRuntimeView(body []byte) (controllermeta.SlotRuntimeView, []byte, error) {
	slotID, rest, err := readUint32(body)
	if err != nil {
		return controllermeta.SlotRuntimeView{}, nil, err
	}
	currentPeers, rest, err := readUint64Slice(rest)
	if err != nil {
		return controllermeta.SlotRuntimeView{}, nil, err
	}
	leaderID, rest, err := readUint64(rest)
	if err != nil {
		return controllermeta.SlotRuntimeView{}, nil, err
	}
	healthyVoters, rest, err := readUint32(rest)
	if err != nil {
		return controllermeta.SlotRuntimeView{}, nil, err
	}
	if len(rest) < 1 {
		return controllermeta.SlotRuntimeView{}, nil, ErrInvalidConfig
	}
	hasQuorum := rest[0] == 1
	observedConfigEpoch, rest, err := readUint64(rest[1:])
	if err != nil {
		return controllermeta.SlotRuntimeView{}, nil, err
	}
	lastReportAtUnix, rest, err := readInt64(rest)
	if err != nil {
		return controllermeta.SlotRuntimeView{}, nil, err
	}
	return controllermeta.SlotRuntimeView{
		SlotID:              slotID,
		CurrentPeers:        currentPeers,
		LeaderID:            leaderID,
		HealthyVoters:       healthyVoters,
		HasQuorum:           hasQuorum,
		ObservedConfigEpoch: observedConfigEpoch,
		LastReportAt:        time.Unix(0, lastReportAtUnix),
	}, rest, nil
}

func encodeReconcileTask(task controllermeta.ReconcileTask) []byte {
	body := make([]byte, 0, 40+len(task.LastError))
	body = binary.BigEndian.AppendUint32(body, task.SlotID)
	body = append(body, byte(task.Kind), byte(task.Step))
	body = binary.BigEndian.AppendUint64(body, task.SourceNode)
	body = binary.BigEndian.AppendUint64(body, task.TargetNode)
	body = binary.BigEndian.AppendUint32(body, task.Attempt)
	body = appendInt64(body, task.NextRunAt.UnixNano())
	body = append(body, byte(task.Status))
	return appendString(body, task.LastError)
}

func decodeReconcileTask(body []byte) (controllermeta.ReconcileTask, error) {
	slotID, rest, err := readUint32(body)
	if err != nil {
		return controllermeta.ReconcileTask{}, err
	}
	if len(rest) < 2 {
		return controllermeta.ReconcileTask{}, ErrInvalidConfig
	}
	kind := controllermeta.TaskKind(rest[0])
	step := controllermeta.TaskStep(rest[1])
	sourceNode, rest, err := readUint64(rest[2:])
	if err != nil {
		return controllermeta.ReconcileTask{}, err
	}
	targetNode, rest, err := readUint64(rest)
	if err != nil {
		return controllermeta.ReconcileTask{}, err
	}
	attempt, rest, err := readUint32(rest)
	if err != nil {
		return controllermeta.ReconcileTask{}, err
	}
	nextRunAtUnix, rest, err := readInt64(rest)
	if err != nil {
		return controllermeta.ReconcileTask{}, err
	}
	if len(rest) < 1 {
		return controllermeta.ReconcileTask{}, ErrInvalidConfig
	}
	status := controllermeta.TaskStatus(rest[0])
	lastError, rest, err := readString(rest[1:])
	if err != nil || len(rest) != 0 {
		return controllermeta.ReconcileTask{}, ErrInvalidConfig
	}
	return controllermeta.ReconcileTask{
		SlotID:     slotID,
		Kind:       kind,
		Step:       step,
		SourceNode: sourceNode,
		TargetNode: targetNode,
		Attempt:    attempt,
		NextRunAt:  time.Unix(0, nextRunAtUnix),
		Status:     status,
		LastError:  lastError,
	}, nil
}

func appendString(dst []byte, value string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(value)))
	return append(dst, value...)
}

func readString(src []byte) (string, []byte, error) {
	length, rest, err := readUvarint(src)
	if err != nil {
		return "", nil, err
	}
	if len(rest) < int(length) {
		return "", nil, ErrInvalidConfig
	}
	return string(rest[:length]), rest[length:], nil
}

func appendUint64Slice(dst []byte, values []uint64) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(values)))
	for _, value := range values {
		dst = binary.BigEndian.AppendUint64(dst, value)
	}
	return dst
}

func readUint64Slice(src []byte) ([]uint64, []byte, error) {
	count, rest, err := readUvarint(src)
	if err != nil {
		return nil, nil, err
	}
	values := make([]uint64, 0, count)
	for i := uint64(0); i < count; i++ {
		value, next, err := readUint64(rest)
		if err != nil {
			return nil, nil, err
		}
		values = append(values, value)
		rest = next
	}
	return values, rest, nil
}

func appendInt64(dst []byte, value int64) []byte {
	return binary.BigEndian.AppendUint64(dst, uint64(value))
}

func readInt64(src []byte) (int64, []byte, error) {
	value, rest, err := readUint64(src)
	return int64(value), rest, err
}

func readUvarint(src []byte) (uint64, []byte, error) {
	value, n := binary.Uvarint(src)
	if n <= 0 {
		return 0, nil, ErrInvalidConfig
	}
	return value, src[n:], nil
}

func readUint64(src []byte) (uint64, []byte, error) {
	if len(src) < 8 {
		return 0, nil, ErrInvalidConfig
	}
	return binary.BigEndian.Uint64(src[:8]), src[8:], nil
}

func readUint32(src []byte) (uint32, []byte, error) {
	if len(src) < 4 {
		return 0, nil, ErrInvalidConfig
	}
	return binary.BigEndian.Uint32(src[:4]), src[4:], nil
}
