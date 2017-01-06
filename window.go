package hvwindow

import (
	"errors"
	"io"
	"sync"
	"time"

	"github.com/fuserobotics/reporter/util"
	"github.com/fuserobotics/reporter/view"
	"github.com/fuserobotics/rstream"
	sstream "github.com/fuserobotics/statestream"
	"golang.org/x/net/context"
)

type HistorianViewWindow struct {
	*rstream.StandardWindow
	data                *sstream.MemoryBackend
	client              view.ReporterServiceClient
	midTimestamp        *time.Time
	initOnce            sync.Once
	shouldActivate      bool
	windowContext       context.Context
	windowContextCancel context.CancelFunc
	stateContext        *view.StateContext

	pendingMeta     *rstream.WindowMeta
	startBoundEntry *sstream.StreamEntry
	endBoundEntry   *sstream.StreamEntry
}

func NewHistorianViewWindow(client view.ReporterServiceClient,
	ctx context.Context,
	stateCtx *view.StateContext) *HistorianViewWindow {
	windowCtx, cancel := context.WithCancel(ctx)
	return &HistorianViewWindow{
		client:              client,
		data:                &sstream.MemoryBackend{},
		windowContext:       windowCtx,
		windowContextCancel: cancel,
		stateContext:        stateCtx,
		StandardWindow:      rstream.NewStandardWindow(),
		pendingMeta:         &rstream.WindowMeta{},
	}
}

func (w *HistorianViewWindow) Data() sstream.StorageBackend {
	return w.data
}

func (w *HistorianViewWindow) InitLive() {
	w.initOnce.Do(func() {
		w.midTimestamp = nil
		go w.doRequest()
	})
}

func (w *HistorianViewWindow) InitWithMidTimestamp(midTimestamp time.Time) {
	w.initOnce.Do(func() {
		w.midTimestamp = &midTimestamp
		go w.doRequest()
	})
}

func (w *HistorianViewWindow) InitWithMetadata(meta *rstream.WindowMeta) {
	w.initOnce.Do(func() {
		w.StandardWindow.InitWithMetadata(meta)
		if w.shouldActivate {
			w.Activate()
		}
	})
}

func (w *HistorianViewWindow) Activate() {
	if w.Meta() == nil {
		w.shouldActivate = true
		return
	}
	w.shouldActivate = false
	currState := w.State()
	if currState == rstream.WindowState_Waiting {
		w.NextState(rstream.WindowState_Pulling)
		go w.doRequest()
	}
}

func (w *HistorianViewWindow) doRequest() (fetchError error) {
	callCtx, callCancel := context.WithCancel(w.windowContext)
	defer func() {
		callCancel()
		if fetchError != nil {
			w.SetError(fetchError)
		}
	}()

	latestTime := w.midTimestamp
	if latestTime == nil {
		latestTimeL := time.Now()
		latestTime = &latestTimeL
	}
	res, err := w.client.GetBoundedStateHistory(callCtx, &view.BoundedStateHistoryRequest{
		BoundsOnly:   w.State() == rstream.WindowState_Pending,
		Context:      w.stateContext,
		MidTimestamp: util.TimeToNumber(*latestTime),
		Mode:         view.BoundedStateHistoryRequest_SNAPSHOT_BOUND,
	})
	if err != nil {
		return err
	}

	for {
		msg, err := res.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		// Will return nil if msg.State is nil
		state, err := msg.State.DecodeStreamEntry()
		if err != nil {
			return err
		}
		switch msg.Status {
		case view.BoundedStateHistoryResponse_BOUNDED_HISTORY_START_BOUND:
			w.startBoundEntry = state
			if w.startBoundEntry == nil {
				return rstream.WindowOutOfRangeErr
			}
			if w.pendingMeta != nil {
				ts := util.NumberToTime(msg.State.Timestamp)
				w.pendingMeta.StartBound = &ts
			}
			currState := w.State()
			if currState == rstream.WindowState_Pulling || currState == rstream.WindowState_Live {
				if err := w.data.SaveEntry(state); err != nil {
					return err
				}
			}
		case view.BoundedStateHistoryResponse_BOUNDED_HISTORY_END_BOUND:
			w.endBoundEntry = state
			if w.endBoundEntry != nil && w.pendingMeta != nil {
				ts := util.NumberToTime(msg.State.Timestamp)
				w.pendingMeta.EndBound = &ts
			}
			currState := w.State()
			if currState == rstream.WindowState_Pending {
				w.NextMeta(w.pendingMeta)
				w.pendingMeta = nil
				w.NextState(rstream.WindowState_Waiting)
				if w.shouldActivate {
					go w.Activate()
				}
			} else if currState == rstream.WindowState_Live {
				if w.endBoundEntry != nil {
					if err := w.data.SaveEntry(w.endBoundEntry); err != nil {
						return err
					}
				}
				w.NextState(rstream.WindowState_Committed)
			}
			return nil
		case view.BoundedStateHistoryResponse_BOUNDED_HISTORY_INITIAL_SET:
			currState := w.State()
			if currState != rstream.WindowState_Pulling && currState != rstream.WindowState_Live {
				break
			}
			if err := w.data.SaveEntry(state); err != nil {
				return err
			}
		case view.BoundedStateHistoryResponse_BOUNDED_HISTORY_TAIL:
			if state == nil {
				if w.Meta().EndBound != nil {
					if err := w.data.SaveEntry(state); err != nil {
						return err
					}
					w.NextState(rstream.WindowState_Committed)
					return nil
				} else {
					w.NextState(rstream.WindowState_Live)
				}
				break
			}
			if err := w.data.SaveEntry(state); err != nil {
				return err
			}
		}
	}
	return errors.New("Session ended unexpectedly.")
}

func (w *HistorianViewWindow) Dispose() {
	w.windowContextCancel()
	w.StandardWindow.Dispose()
}
