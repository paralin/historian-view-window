package hvwindow

import (
	"strconv"
	"sync"

	"github.com/fuserobotics/reporter/view"
	"github.com/fuserobotics/rstream"
	"golang.org/x/net/context"
)

type historianServiceReference struct {
	Identifier string
	Client     view.ReporterServiceClient
	Factory    rstream.WindowFactory
	Cancel     context.CancelFunc
}

type HistorianViewMultiplexer struct {
	stateCtx *view.StateContext

	serviceMtx          sync.RWMutex
	serviceReferences   []*historianServiceReference
	serviceReferenceCtr int

	windowMtx  sync.RWMutex
	windows    map[string]*rstream.WindowMultiplexer
	windowsCtr int

	multiplexerCtx       context.Context
	multiplexerCtxCancel context.CancelFunc

	disposed   bool
	disposeMtx sync.Mutex
}

func NewHistorianViewMultiplexer(ctx context.Context, stateCtx *view.StateContext) *HistorianViewMultiplexer {
	res := &HistorianViewMultiplexer{stateCtx: stateCtx}
	res.multiplexerCtx, res.multiplexerCtxCancel = context.WithCancel(ctx)
	return res
}

func (m *HistorianViewMultiplexer) AddService(client view.ReporterServiceClient) {
	m.disposeMtx.Lock()
	defer m.disposeMtx.Unlock()

	if m.disposed {
		return
	}

	m.serviceMtx.Lock()
	defer m.serviceMtx.Unlock()

	if client == nil {
		return
	}
	for _, ref := range m.serviceReferences {
		if ref.Client == client {
			return
		}
	}

	m.serviceReferenceCtr++
	nctx, cancel := context.WithCancel(m.multiplexerCtx)
	id := strconv.Itoa(m.serviceReferenceCtr)
	nref := &historianServiceReference{
		Identifier: id,
		Client:     client,
		Cancel:     cancel,
		Factory: func(ctx context.Context) rstream.Window {
			return NewHistorianViewWindow(client, nctx, m.stateCtx)
		},
	}
	m.serviceReferences = append(m.serviceReferences, nref)
}

func (m *HistorianViewMultiplexer) RemoveService(client view.ReporterServiceClient) {
	m.disposeMtx.Lock()
	defer m.disposeMtx.Unlock()

	if m.disposed {
		return
	}

	m.serviceMtx.Lock()
	defer m.serviceMtx.Unlock()

	if client == nil {
		return
	}

	var oldRef *historianServiceReference
	var oldIdx int
	for idx, ref := range m.serviceReferences {
		if ref.Client == client {
			oldIdx = idx
			oldRef = ref
			break
		}
	}
	if oldRef == nil {
		return
	}

	m.windowMtx.RLock()
	defer m.windowMtx.RUnlock()

	for _, wind := range m.windows {
		wind.DeleteFactory(oldRef.Identifier)
	}
	oldRef.Cancel()
	m.serviceReferences[oldIdx] = m.serviceReferences[len(m.serviceReferences)-1]
	m.serviceReferences = m.serviceReferences[:len(m.serviceReferences)-1]
}

func (m *HistorianViewMultiplexer) Factory(ctx context.Context) rstream.Window {
	m.disposeMtx.Lock()
	defer m.disposeMtx.Unlock()

	if m.disposed {
		return nil
	}

	nwind := rstream.NewWindowMultiplexer(m.multiplexerCtx)
	m.serviceMtx.RLock()
	defer m.serviceMtx.RUnlock()

	for _, serv := range m.serviceReferences {
		nwind.AddFactory(&rstream.WindowFactoryReference{
			Factory:    serv.Factory,
			Identifier: serv.Identifier,
		})
	}

	m.windowMtx.Lock()
	defer m.windowMtx.Unlock()

	m.windowsCtr++
	id := strconv.Itoa(m.windowsCtr)
	m.windows[id] = nwind

	nwind.OnDisposed(func() {
		m.windowMtx.Lock()
		defer m.windowMtx.Unlock()

		delete(m.windows, id)
	})

	return nwind
}

func (m *HistorianViewMultiplexer) Dispose() {
	m.disposeMtx.Lock()
	defer m.disposeMtx.Unlock()

	if m.disposed {
		return
	}

	m.disposed = true
	m.multiplexerCtxCancel()
	for _, wind := range m.windows {
		wind.Dispose()
	}
	m.windows = nil

	for _, serv := range m.serviceReferences {
		serv.Cancel()
	}
	m.serviceReferences = nil
}
