package hvwindow

import (
	"time"

	"github.com/fuserobotics/rstream"
)

type HistorianViewWindow struct {
	*rstream.StandardWindow
}

func NewHistorianViewWindow() *HistorianViewWindow {
	return &HistorianViewWindow{
		StandardWindow: rstream.NewStandardWindow(),
	}
}

func (w *HistorianViewWindow) InitLive() {
}

func (w *HistorianViewWindow) InitWithMidTimestamp(midTimestamp time.Time) {
}

func (w *HistorianViewWindow) Activate() {
}
