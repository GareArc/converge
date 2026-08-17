package converge

type runModeKind int

const (
	runModeUnset runModeKind = iota
	runModeOneReplica
	runModeSplit
	runModeAllReplicas
)

type RunMode struct{ kind runModeKind }

var (
	OnOneReplica        = RunMode{runModeOneReplica}
	SplitAcrossReplicas = RunMode{runModeSplit}
	OnAllReplicas       = RunMode{runModeAllReplicas}
)

func (m RunMode) IsZero() bool { return m.kind == runModeUnset }

func (m RunMode) String() string {
	switch m.kind {
	case runModeOneReplica:
		return "OnOneReplica"
	case runModeSplit:
		return "SplitAcrossReplicas"
	case runModeAllReplicas:
		return "OnAllReplicas"
	default:
		return "unset"
	}
}

type Surface int

const (
	SurfaceReconcile Surface = iota + 1
	SurfaceWorker
)

func (s Surface) String() string {
	switch s {
	case SurfaceReconcile:
		return "reconcile"
	case SurfaceWorker:
		return "worker"
	default:
		return "unknown"
	}
}

type deliveryModeKind int

const (
	deliveryUnset deliveryModeKind = iota
	deliveryGroup
	deliveryBroadcast
)

type DeliveryMode struct{ kind deliveryModeKind }

var (
	Group     = DeliveryMode{deliveryGroup}
	Broadcast = DeliveryMode{deliveryBroadcast}
)

func (d DeliveryMode) IsZero() bool { return d.kind == deliveryUnset }

func (d DeliveryMode) String() string {
	switch d.kind {
	case deliveryGroup:
		return "Group"
	case deliveryBroadcast:
		return "Broadcast"
	default:
		return "unset"
	}
}
