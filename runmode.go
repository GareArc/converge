package converge

type runModeKind int

const (
	runModeUnset runModeKind = iota
	runModeOneReplica
	runModeAllReplicas
	runModeCompeting
)

type RunMode struct{ kind runModeKind }

var (
	OnOneReplica  = RunMode{runModeOneReplica}
	OnAllReplicas = RunMode{runModeAllReplicas}
	Competing     = RunMode{runModeCompeting}
)

func (m RunMode) IsZero() bool { return m.kind == runModeUnset }

func (m RunMode) String() string {
	switch m.kind {
	case runModeOneReplica:
		return "OnOneReplica"
	case runModeAllReplicas:
		return "OnAllReplicas"
	case runModeCompeting:
		return "Competing"
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
