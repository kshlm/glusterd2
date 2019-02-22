package bitrot

import (
	"github.com/gluster/glusterd2/glusterd2/oldtransaction"
	"github.com/gluster/glusterd2/glusterd2/servers/rest/route"
)

const name = "bitrot"

// Plugin is a structure which implements GlusterdPlugin interface
type Plugin struct {
}

// Name returns name of plugin
func (p *Plugin) Name() string {
	return name
}

// RestRoutes returns list of REST API routes to register with Glusterd
func (p *Plugin) RestRoutes() route.Routes {
	return route.Routes{
		route.Route{
			Name:        "BitrotEnable",
			Method:      "POST",
			Pattern:     "/volumes/{volname}/bitrot/enable",
			Version:     1,
			HandlerFunc: bitrotEnableHandler},
		route.Route{
			Name:        "BitrotDisable",
			Method:      "POST",
			Pattern:     "/volumes/{volname}/bitrot/disable",
			Version:     1,
			HandlerFunc: bitrotDisableHandler},
		route.Route{
			Name:        "BitrotScrubOndemand",
			Method:      "POST",
			Pattern:     "/volumes/{volname}/bitrot/scrubondemand",
			Version:     1,
			HandlerFunc: bitrotScrubOndemandHandler},
		route.Route{
			Name:        "BitrotScrubStatus",
			Method:      "GET",
			Pattern:     "/volumes/{volname}/bitrot/scrubstatus",
			Version:     1,
			HandlerFunc: bitrotScrubStatusHandler},
	}
}

// RegisterStepFuncs registers transaction step functions with
// Glusterd Transaction framework
func (p *Plugin) RegisterStepFuncs() {
	oldtransaction.RegisterStepFunc(txnBitrotEnableDisable, "bitrot-enable.Commit")
	oldtransaction.RegisterStepFunc(txnBitrotEnableDisable, "bitrot-disable.Commit")
	oldtransaction.RegisterStepFunc(txnBitrotScrubOndemand, "bitrot-scrubondemand.Commit")
	oldtransaction.RegisterStepFunc(txnBitrotScrubStatus, "bitrot-scrubstatus.Commit")
	return
}
