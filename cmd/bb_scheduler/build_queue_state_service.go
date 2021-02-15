package main

import (
	"context"
	"encoding/json"
	"html/template"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/buildqueuestate"
	re_util "github.com/buildbarn/bb-remote-execution/pkg/util"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/gorilla/mux"

	"google.golang.org/grpc/status"
)

const (
	// pageSize controls how many elements are showed in pages
	// containing listings of workers, operations, etc. These pages
	// can become quite big for large setups.
	pageSize = 1000
)

var (
	jsonpbMarshaler         = jsonpb.Marshaler{}
	jsonpbIndentedMarshaler = jsonpb.Marshaler{Indent: "  "}

	templateFuncMap = template.FuncMap{
		"abbreviate": func(s string) string {
			if len(s) > 11 {
				return s[:8] + "..."
			}
			return s
		},
		"action_url": func(browserURL *url.URL, instanceName string, actionDigest *remoteexecution.Digest) string {
			i, err := digest.NewInstanceName(instanceName)
			if err != nil {
				return ""
			}
			d, err := i.NewDigestFromProto(actionDigest)
			if err != nil {
				return ""
			}
			return re_util.GetBrowserURL(browserURL, "action", d)
		},
		"operation_stage_queued": func(o *buildqueuestate.OperationState) *buildqueuestate.OperationState_Queued {
			if s, ok := o.Stage.(*buildqueuestate.OperationState_Queued_); ok {
				return s.Queued
			}
			return nil
		},
		"operation_stage_executing": func(o *buildqueuestate.OperationState) *empty.Empty {
			if s, ok := o.Stage.(*buildqueuestate.OperationState_Executing); ok {
				return s.Executing
			}
			return nil
		},
		"operation_stage_completed": func(o *buildqueuestate.OperationState) *remoteexecution.ExecuteResponse {
			if s, ok := o.Stage.(*buildqueuestate.OperationState_Completed); ok {
				return s.Completed
			}
			return nil
		},
		"proto_to_json":          jsonpbMarshaler.MarshalToString,
		"proto_to_indented_json": jsonpbIndentedMarshaler.MarshalToString,
		"error_proto":            status.ErrorProto,
		"time_future": func(t *timestamp.Timestamp, now time.Time) string {
			if t == nil {
				return "∞"
			}
			ts, err := ptypes.Timestamp(t)
			if err != nil {
				return "?"
			}
			return ts.Sub(now).Truncate(time.Second).String()
		},
		"time_past": func(t *timestamp.Timestamp, now time.Time) string {
			ts, err := ptypes.Timestamp(t)
			if err != nil {
				return "?"
			}
			return now.Sub(ts).Truncate(time.Second).String()
		},
		"to_json": func(v interface{}) (string, error) {
			b, err := json.Marshal(v)
			if err != nil {
				return "", err
			}
			return string(b), nil
		},
		"to_background_color": func(s string) string {
			return "#" + s[:6]
		},
		"to_foreground_color": func(s string) string {
			return "#" + invertColor(s[:2]) + invertColor(s[2:4]) + invertColor(s[4:6])
		},
	}

	getBuildQueueStateTemplate = template.Must(template.New("GetBuildQueueState").Funcs(templateFuncMap).Parse(`
<!DOCTYPE html>
<html>
  <head>
    <title>Buildbarn Scheduler</title>
    <style>
      html { font-family: sans-serif; }
      table { border-collapse: collapse; }
      table, td, th { border: 1px solid black; }
      td, th { padding-left: 5px; padding-right: 5px; }
    </style>
  </head>
  <body>
    <h1>Buildbarn Scheduler</h1>
    <p>Total number of operations: <a href="operations">{{.OperationsCount}}</a></p>
    <h2>Platform queues</h2>
    <table>
      <thead>
        <tr>
          <th>Instance name</th>
          <th>Platform</th>
          <th>Timeout</th>
          <th>Queued invocations</th>
          <th>Executing workers</th>
          <th>Drains</th>
        </tr>
      </thead>
      {{$now := .Now}}
      {{range .PlatformQueues}}
        {{$platform := proto_to_json .Name.Platform}}
        <tr>
          <td>{{.Name.GetInstanceName | printf "%#v"}}</td>
          <td>{{$platform}}</td>
          <td>{{time_future .Timeout $now}}</td>
          {{$platformQueueName := proto_to_json .Name}}
          <td>
            <a href="invocations?platform_queue_name={{$platformQueueName}}&amp;just_queued_invocations=true">{{.QueuedInvocationsCount}}</a>
            /
            <a href="invocations?platform_queue_name={{$platformQueueName}}">{{.InvocationsCount}}</a>
          </td>
          <td>
            <a href="workers?platform_queue_name={{$platformQueueName}}&amp;just_executing_workers=true">{{.ExecutingWorkersCount}}</a>
            /
            <a href="workers?platform_queue_name={{$platformQueueName}}">{{.WorkersCount}}</a>
          </td>
          <td>
            <a href="drains?platform_queue_name={{$platformQueueName}}">{{.DrainsCount}}</a>
          </td>
        </tr>
      {{end}}
    </table>
  </body>
</html>
`))
	getOperationStateTemplate = template.Must(template.New("GetOperationState").Funcs(templateFuncMap).Parse(`
<!DOCTYPE html>
<html>
  <head>
    <title>Operation {{.OperationName}}</title>
    <style>
      html { font-family: sans-serif; }
    </style>
  </head>
  <body>
    <h1>Operation {{.OperationName}}</h1>
    {{$now := .Now}}
    <p>Instance name: {{.Operation.PlatformQueueName.InstanceName | printf "%#v"}}<br/>
    Platform: {{proto_to_json .Operation.PlatformQueueName.Platform}}<br/>
    Invocation ID: {{.Operation.InvocationId | printf "%#v"}}<br/>
    Action digest: <a href="{{action_url .BrowserURL .Operation.PlatformQueueName.InstanceName .Operation.ActionDigest}}">{{proto_to_json .Operation.ActionDigest}}</a><br/>
    Age: {{time_past .Operation.QueuedTimestamp $now}}<br/>
    Timeout: {{time_future .Operation.Timeout $now}}<br/>
    Argv[0]: {{.Operation.Argv0}}<br/>
    Stage:
      {{with operation_stage_queued .Operation}}
        Queued at priority {{.Priority}}
      {{else}}
        {{with operation_stage_executing .Operation}}
          Executing
        {{else}}
          {{with operation_stage_completed .Operation}}
            {{with error_proto .Status}}
              Failed with {{.}}
            {{else}}
              {{with .Result}}
                Completed with exit code {{.ExitCode}}
              {{else}}
                Action result missing
              {{end}}
            {{end}}
          {{else}}
            Unknown
          {{end}}
        {{end}}
      {{end}}
    </p>
    {{with operation_stage_completed .Operation}}
      <h2>Execute response</h2>
      <pre>{{proto_to_indented_json .}}</pre>
    {{else}}
      <form action="kill_operation" method="post">
        <p>
          <input name="name" type="hidden" value="{{.OperationName}}"/>
          <input type="submit" value="Kill operation"/>
        </p>
      </form>
    {{end}}
  </body>
</html>
`))
	listOperationStateTemplate = template.Must(template.New("ListOperationState").Funcs(templateFuncMap).Parse(`
<!DOCTYPE html>
<html>
  <head>
    <title>All operations</title>
    <style>
      html { font-family: sans-serif; }
      table { border-collapse: collapse; }
      table, td, th { border: 1px solid black; }
      td, th { padding-left: 5px; padding-right: 5px; }
      .text-monospace { font-family: monospace; }
    </style>
  </head>
  <body>
    <h1>All operations</h1>
    <p>Showing operations [{{.PaginationInfo.StartIndex}}, {{.EndIndex}}) of {{.PaginationInfo.TotalEntries}} in total.
      {{with .StartAfter}}
        <a href="?start_after={{proto_to_json .}}">&gt;&gt;&gt;</a>
      {{end}}
    </p>
    <table>
      <thead>
        <tr>
          <th>Timeout</th>
          <th>Operation name</th>
          <th>Action digest</th>
          <th>Argv[0]</th>
          <th>Stage</th>
        </tr>
      </thead>
      {{$browserURL := .BrowserURL}}
      {{$now := .Now}}
      {{range .Operations}}
        <tr>
          <td>{{time_future .Timeout $now}}</td>
          <td style="background-color: {{to_background_color .Name}}">
            <a class="text-monospace" href="operation?name={{.Name}}" style="color: {{to_foreground_color .Name}}">{{abbreviate .Name}}</a>
          </td>
          <td style="background-color: {{to_background_color .ActionDigest.Hash}}">
            <a class="text-monospace" href="{{action_url $browserURL .PlatformQueueName.InstanceName .ActionDigest}}" style="color: {{to_foreground_color .ActionDigest.Hash}}">{{abbreviate .ActionDigest.Hash}}</a>
          </td>
          <td>{{.Argv0}}</td>
          {{with operation_stage_queued .}}
            <td>Queued at priority {{.Priority}}</td>
          {{else}}
            {{with operation_stage_executing .}}
              <td style="background-color: lightblue">Executing</td>
            {{else}}
              {{with operation_stage_completed .}}
                {{with error_proto .Status}}
                  <td style="background-color: red">Failed with {{.}}</td>
                {{else}}
                  {{with .Result}}
                    {{if eq .ExitCode 0}}
                      <td style="background-color: lightgreen">Completed with exit code {{.ExitCode}}</td>
                    {{else}}
                      <td style="background-color: orange">Completed with exit code {{.ExitCode}}</td>
                    {{end}}
                  {{else}}
                    <td style="background-color: red">Action result missing</td>
                  {{end}}
                {{end}}
              {{else}}
                <td style="background-color: red">Unknown</td>
              {{end}}
            {{end}}
          {{end}}
        </tr>
      {{end}}
    </table>
  </body>
</html>
`))
	listInvocationStateTemplate = template.Must(template.New("ListInvocationState").Funcs(templateFuncMap).Parse(`
<!DOCTYPE html>
<html>
  <head>
    <title>{{if .JustQueuedInvocations}}Queued{{else}}All{{end}} invocations</title>
    <style>
      html { font-family: sans-serif; }
      table { border-collapse: collapse; }
      table, td, th { border: 1px solid black; }
      td, th { padding-left: 5px; padding-right: 5px; }
      .text-monospace { font-family: monospace; }
    </style>
  </head>
  <body>
    <h1>{{if .JustQueuedInvocations}}Queued{{else}}All{{end}} invocations</h1>
    <p>Instance name: {{.PlatformQueueName.InstanceName | printf "%#v"}}<br/>
    Platform: {{proto_to_json .PlatformQueueName.Platform}}</p>
    <table>
      <thead>
        <tr>
          <th>Invocation ID</th>
          <th>Queued operations</th>
          <th>First priority</th>
          <th>First age</th>
          <th>Executing operations</th>
        </tr>
      </thead>
      {{$now := .Now}}
      {{$platformQueueName := .PlatformQueueName}}
      {{range .Invocations}}
        <tr>
          <td>{{.Id | printf "%#v"}}</td>
          <td><a href="queued_operations?platform_queue_name={{proto_to_json $platformQueueName}}&amp;invocation_id={{.Id}}">{{.QueuedOperationsCount}}</a></td>
          {{with .FirstQueuedOperation}}
            <td>{{with operation_stage_queued .}}{{.Priority}}{{else}}Unknown{{end}}</td>
            <td>{{time_past .QueuedTimestamp $now}}</td>
          {{else}}
            <td colspan="2">No operations queued</td>
          {{end}}
          <td>{{.ExecutingOperationsCount}}</td>
        </tr>
      {{end}}
    </table>
  </body>
</html>
`))
	listQueuedOperationStateTemplate = template.Must(template.New("ListQueuedOperationState").Funcs(templateFuncMap).Parse(`
<!DOCTYPE html>
<html>
  <head>
    <title>Queued operations</title>
    <style>
      html { font-family: sans-serif; }
      table { border-collapse: collapse; }
      table, td, th { border: 1px solid black; }
      td, th { padding-left: 5px; padding-right: 5px; }
      .text-monospace { font-family: monospace; }
    </style>
  </head>
  <body>
    <h1>Queued operations</h1>
    {{$platformQueueName := .PlatformQueueName}}
    <p>Instance name: {{$platformQueueName.InstanceName | printf "%#v"}}<br/>
    Platform: {{proto_to_json $platformQueueName.Platform}}<br/>
    {{$invocationID := .InvocationID}}
    Invocation ID: {{$invocationID | printf "%#v"}}</p>
    <p>Showing queued operations [{{.PaginationInfo.StartIndex}}, {{.EndIndex}}) of {{.PaginationInfo.TotalEntries}} in total.
      {{with .StartAfter}}
        <a href="?platform_queue_name={{proto_to_json $platformQueueName}}&amp;invocation_id={{$invocationID}}&amp;start_after={{proto_to_json .}}">&gt;&gt;&gt;</a>
      {{end}}
    </p>
    <table>
      <thead>
        <tr>
          <th>Priority</th>
          <th>Age</th>
          <th>Timeout</th>
          <th>Operation name</th>
          <th>Action digest</th>
          <th>Argv[0]</th>
        </tr>
      </thead>
      {{$browserURL := .BrowserURL}}
      {{$now := .Now}}
      {{range .QueuedOperations}}
        <tr>
          <td>{{with operation_stage_queued .}}{{.Priority}}{{else}}Unknown{{end}}</td>
          <td>{{time_past .QueuedTimestamp $now}}</td>
          <td>{{time_future .Timeout $now}}</td>
          <td style="background-color: {{to_background_color .Name}}">
            <a class="text-monospace" href="operation?name={{.Name}}" style="color: {{to_foreground_color .Name}}">{{abbreviate .Name}}</a>
          </td>
          <td style="background-color: {{to_background_color .ActionDigest.Hash}}">
            <a class="text-monospace" href="{{action_url $browserURL $platformQueueName.InstanceName .ActionDigest}}" style="color: {{to_foreground_color .ActionDigest.Hash}}">{{abbreviate .ActionDigest.Hash}}</a>
          </td>
          <td>{{.Argv0}}</td>
        </tr>
      {{end}}
    </table>
  </body>
</html>
`))
	listWorkerStateTemplate = template.Must(template.New("ListWorkerState").Funcs(templateFuncMap).Parse(`
<!DOCTYPE html>
<html>
  <head>
    <title>{{if .JustExecutingWorkers}}Executing{{else}}All{{end}} workers</title>
    <style>
      html { font-family: sans-serif; }
      table { border-collapse: collapse; }
      table, td, th { border: 1px solid black; }
      td, th { padding-left: 5px; padding-right: 5px; }
      .text-monospace { font-family: monospace; }
    </style>
  </head>
  <body>
    <h1>{{if .JustExecutingWorkers}}Executing{{else}}All{{end}} workers</h1>
    {{$platformQueueName := .PlatformQueueName}}
    <p>Instance name: {{$platformQueueName.InstanceName | printf "%#v"}}<br/>
    Platform: {{proto_to_json $platformQueueName.Platform}}</p>
    <p>Showing workers [{{.PaginationInfo.StartIndex}}, {{.EndIndex}}) of {{.PaginationInfo.TotalEntries}} in total.
      {{with .StartAfter}}
        <a href="?platform_queue_name={{proto_to_json $platformQueueName}}&amp;start_after={{proto_to_json .}}">&gt;&gt;&gt;</a>
      {{end}}
    </p>
    <table>
      <thead>
        <tr>
          <th>Worker ID</th>
          <th>Worker timeout</th>
          <th>Operation timeout</th>
          <th>Operation name</th>
          <th>Action digest</th>
          <th>Argv[0]</th>
        </tr>
      </thead>
      {{$browserURL := .BrowserURL}}
      {{$now := .Now}}
      {{range .Workers}}
        {{$workerID := to_json .Id}}
        <tr>
          <td>{{$workerID}}</td>
          <td>{{with .Timeout}}{{time_future . $now}}{{else}}∞{{end}}</td>
          {{with .CurrentOperation}}
            <td>{{with .Timeout}}{{time_future . $now}}{{else}}∞{{end}}</td>
            <td style="background-color: {{to_background_color .Name}}">
              <a class="text-monospace" href="operation?name={{.Name}}" style="color: {{to_foreground_color .Name}}">{{abbreviate .Name}}</a>
            </td>
            <td style="background-color: {{to_background_color .ActionDigest.Hash}}">
              <a class="text-monospace" href="{{action_url $browserURL $platformQueueName.InstanceName .ActionDigest}}" style="color: {{to_foreground_color .ActionDigest.Hash}}">{{abbreviate .ActionDigest.Hash}}</a>
            </td>
            <td>{{.Argv0}}</td>
          {{else}}
            <td colspan="4">{{if .Drained}}drained{{else}}idle{{end}}</td>
          {{end}}
        </tr>
      {{end}}
    </table>
  </body>
</html>
`))
	listDrainStateTemplate = template.Must(template.New("ListDrainState").Funcs(templateFuncMap).Parse(`
<!DOCTYPE html>
<html>
  <head>
    <title>Drains</title>
    <style>
      html { font-family: sans-serif; }
      table { border-collapse: collapse; }
      table, td, th { border: 1px solid black; }
      td, th { padding-left: 5px; padding-right: 5px; }
    </style>
  </head>
  <body>
    <h1>Drains</h1>
    <p>Instance name: {{.PlatformQueueName.InstanceName | printf "%#v"}}<br/>
    Platform: {{proto_to_json .PlatformQueueName.Platform}}</p>
    {{$platformQueueName := proto_to_json .PlatformQueueName}}
    <table>
      <thead>
        <tr>
          <th>Worker ID pattern</th>
          <th>Age</th>
          <th>Actions</th>
        </tr>
      </thead>
      {{$now := .Now}}
      {{range .Drains}}
        {{$workerIDPattern := to_json .WorkerIdPattern}}
        <tr>
          <td>{{$workerIDPattern}}</td>
          <td>{{time_past .CreatedTimestamp $now}}</td>
          <td>
            <form action="remove_drain" method="post">
              <input name="platform_queue_name" type="hidden" value="{{$platformQueueName}}"/>
              <input name="worker_id_pattern" type="hidden" value="{{$workerIDPattern}}"/>
              <input type="submit" value="Remove"/>
            </form>
          </td>
        </tr>
      {{end}}
    </table>
    <form action="add_drain" method="post">
      <p>
        <input name="platform_queue_name" type="hidden" value="{{$platformQueueName}}"/>
        <input name="worker_id_pattern" type="text"/>
        <input type="submit" value="Create drain"/>
      </p>
    </form>
  </body>
</html>
`))
)

// invertColor takes a single red, green or blue color value and
// transforms it to its high contrast counterpart. This color can be
// used to display high contrast text on top of an arbitrarily colored
// background.
func invertColor(s string) string {
	if r, _ := strconv.ParseInt(s, 16, 0); r < 128 {
		return "ff"
	}
	return "00"
}

type buildQueueStateService struct {
	buildQueue buildqueuestate.BuildQueueStateServer
	clock      clock.Clock
	browserURL *url.URL
}

func newBuildQueueStateService(buildQueue buildqueuestate.BuildQueueStateServer, clock clock.Clock, browserURL *url.URL, router *mux.Router) *buildQueueStateService {
	s := &buildQueueStateService{
		buildQueue: buildQueue,
		clock:      clock,
		browserURL: browserURL,
	}
	router.HandleFunc("/", s.handleGetBuildQueueState)
	router.HandleFunc("/add_drain", s.handleAddDrain)
	router.HandleFunc("/drains", s.handleListDrains)
	router.HandleFunc("/invocations", s.handleListInvocations)
	router.HandleFunc("/kill_operation", s.handleKillOperation)
	router.HandleFunc("/operation", s.handleGetOperation)
	router.HandleFunc("/operations", s.handleListOperations)
	router.HandleFunc("/queued_operations", s.handleListQueuedOperations)
	router.HandleFunc("/remove_drain", s.handleRemoveDrain)
	router.HandleFunc("/workers", s.handleListWorkers)
	return s
}

func (s *buildQueueStateService) handleGetBuildQueueState(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	operationsCount, err := s.buildQueue.ListOperations(ctx, &buildqueuestate.ListOperationsRequest{})
	if err != nil {
		http.Error(w, util.StatusWrap(err, "Failed to list platform queues").Error(), http.StatusBadRequest)
		return
	}
	response, err := s.buildQueue.ListPlatformQueues(ctx, &empty.Empty{})
	if err != nil {
		http.Error(w, util.StatusWrap(err, "Failed to list platform queues").Error(), http.StatusBadRequest)
		return
	}

	if err := getBuildQueueStateTemplate.Execute(w, struct {
		Now             time.Time
		PlatformQueues  []*buildqueuestate.PlatformQueueState
		OperationsCount uint32
	}{
		Now:             s.clock.Now(),
		PlatformQueues:  response.PlatformQueues,
		OperationsCount: operationsCount.PaginationInfo.TotalEntries,
	}); err != nil {
		log.Print(err)
	}
}

func (s *buildQueueStateService) handleListInvocations(w http.ResponseWriter, req *http.Request) {
	query := req.URL.Query()
	var platformQueueName buildqueuestate.PlatformQueueName
	if err := jsonpb.UnmarshalString(query.Get("platform_queue_name"), &platformQueueName); err != nil {
		http.Error(w, util.StatusWrap(err, "Failed to extract platform queue").Error(), http.StatusBadRequest)
		return
	}
	justQueuedInvocations := query.Get("just_queued_invocations") != ""

	ctx := req.Context()
	response, err := s.buildQueue.ListInvocations(ctx, &buildqueuestate.ListInvocationsRequest{
		PlatformQueueName:     &platformQueueName,
		JustQueuedInvocations: justQueuedInvocations,
	})
	if err != nil {
		// TODO: Pick the right error code.
		http.Error(w, util.StatusWrap(err, "Failed to list invocations state").Error(), http.StatusBadRequest)
		return
	}
	if err := listInvocationStateTemplate.Execute(w, struct {
		PlatformQueueName     *buildqueuestate.PlatformQueueName
		Invocations           []*buildqueuestate.InvocationState
		JustQueuedInvocations bool
		Now                   time.Time
	}{
		PlatformQueueName:     &platformQueueName,
		Invocations:           response.Invocations,
		JustQueuedInvocations: justQueuedInvocations,
		Now:                   s.clock.Now(),
	}); err != nil {
		log.Print(err)
	}
}

func (s *buildQueueStateService) handleKillOperation(w http.ResponseWriter, req *http.Request) {
	req.ParseForm()
	ctx := req.Context()
	if _, err := s.buildQueue.KillOperation(ctx, &buildqueuestate.KillOperationRequest{
		OperationName: req.FormValue("name"),
	}); err != nil {
		http.Error(w, util.StatusWrap(err, "Failed to kill operation").Error(), http.StatusBadRequest)
		return
	}
	http.Redirect(w, req, req.Header.Get("Referer"), http.StatusSeeOther)
}

func (s *buildQueueStateService) handleGetOperation(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	operationName := req.URL.Query().Get("name")
	response, err := s.buildQueue.GetOperation(ctx, &buildqueuestate.GetOperationRequest{
		OperationName: operationName,
	})
	if err != nil {
		http.Error(w, util.StatusWrap(err, "Failed to get operation").Error(), http.StatusBadRequest)
		return
	}
	if err := getOperationStateTemplate.Execute(w, struct {
		BrowserURL    *url.URL
		Now           time.Time
		OperationName string
		Operation     *buildqueuestate.OperationState
	}{
		BrowserURL:    s.browserURL,
		Now:           s.clock.Now(),
		OperationName: operationName,
		Operation:     response.Operation,
	}); err != nil {
		log.Print(err)
	}
}

func (s *buildQueueStateService) handleListOperations(w http.ResponseWriter, req *http.Request) {
	var startAfter *buildqueuestate.ListOperationsRequest_StartAfter
	if startAfterParameter := req.URL.Query().Get("start_after"); startAfterParameter != "" {
		var startAfterMessage buildqueuestate.ListOperationsRequest_StartAfter
		if err := jsonpb.UnmarshalString(startAfterParameter, &startAfterMessage); err != nil {
			http.Error(w, util.StatusWrap(err, "Failed to parse start after message").Error(), http.StatusBadRequest)
			return
		}
		startAfter = &startAfterMessage
	}

	ctx := req.Context()
	response, err := s.buildQueue.ListOperations(ctx, &buildqueuestate.ListOperationsRequest{
		PageSize:   pageSize,
		StartAfter: startAfter,
	})
	if err != nil {
		http.Error(w, util.StatusWrap(err, "Failed to list operations").Error(), http.StatusBadRequest)
		return
	}

	var nextStartAfter *buildqueuestate.ListOperationsRequest_StartAfter
	if l := response.Operations; len(l) > 0 {
		o := l[len(l)-1]
		nextStartAfter = &buildqueuestate.ListOperationsRequest_StartAfter{
			OperationName: o.Name,
		}
	}

	if err := listOperationStateTemplate.Execute(w, struct {
		BrowserURL     *url.URL
		Now            time.Time
		PaginationInfo *buildqueuestate.PaginationInfo
		EndIndex       int
		StartAfter     *buildqueuestate.ListOperationsRequest_StartAfter
		Operations     []*buildqueuestate.OperationState
	}{
		BrowserURL:     s.browserURL,
		Now:            s.clock.Now(),
		PaginationInfo: response.PaginationInfo,
		EndIndex:       int(response.PaginationInfo.StartIndex) + len(response.Operations),
		StartAfter:     nextStartAfter,
		Operations:     response.Operations,
	}); err != nil {
		log.Print(err)
	}
}

func (s *buildQueueStateService) handleListQueuedOperations(w http.ResponseWriter, req *http.Request) {
	query := req.URL.Query()
	var platformQueueName buildqueuestate.PlatformQueueName
	if err := jsonpb.UnmarshalString(query.Get("platform_queue_name"), &platformQueueName); err != nil {
		http.Error(w, util.StatusWrap(err, "Failed to extract platform queue").Error(), http.StatusBadRequest)
		return
	}

	var startAfter *buildqueuestate.ListQueuedOperationsRequest_StartAfter
	if startAfterParameter := req.URL.Query().Get("start_after"); startAfterParameter != "" {
		var startAfterMessage buildqueuestate.ListQueuedOperationsRequest_StartAfter
		if err := jsonpb.UnmarshalString(startAfterParameter, &startAfterMessage); err != nil {
			http.Error(w, util.StatusWrap(err, "Failed to parse start after message").Error(), http.StatusBadRequest)
			return
		}
		startAfter = &startAfterMessage
	}

	ctx := req.Context()
	invocationID := query.Get("invocation_id")
	response, err := s.buildQueue.ListQueuedOperations(ctx, &buildqueuestate.ListQueuedOperationsRequest{
		PlatformQueueName: &platformQueueName,
		InvocationId:      invocationID,
		PageSize:          pageSize,
		StartAfter:        startAfter,
	})
	if err != nil {
		// TODO: Pick the right error code.
		http.Error(w, util.StatusWrap(err, "Failed to list queued operation state").Error(), http.StatusBadRequest)
		return
	}

	var nextStartAfter *buildqueuestate.ListQueuedOperationsRequest_StartAfter
	if l := response.QueuedOperations; len(l) > 0 {
		o := l[len(l)-1]
		nextStartAfter = &buildqueuestate.ListQueuedOperationsRequest_StartAfter{
			Priority:        o.Stage.(*buildqueuestate.OperationState_Queued_).Queued.Priority,
			QueuedTimestamp: o.QueuedTimestamp,
		}
	}

	if err := listQueuedOperationStateTemplate.Execute(w, struct {
		PlatformQueueName *buildqueuestate.PlatformQueueName
		InvocationID      string
		BrowserURL        *url.URL
		Now               time.Time
		PaginationInfo    *buildqueuestate.PaginationInfo
		EndIndex          int
		StartAfter        *buildqueuestate.ListQueuedOperationsRequest_StartAfter
		QueuedOperations  []*buildqueuestate.OperationState
	}{
		PlatformQueueName: &platformQueueName,
		InvocationID:      invocationID,
		BrowserURL:        s.browserURL,
		Now:               s.clock.Now(),
		PaginationInfo:    response.PaginationInfo,
		EndIndex:          int(response.PaginationInfo.StartIndex) + len(response.QueuedOperations),
		StartAfter:        nextStartAfter,
		QueuedOperations:  response.QueuedOperations,
	}); err != nil {
		log.Print(err)
	}
}

func (s *buildQueueStateService) handleListWorkers(w http.ResponseWriter, req *http.Request) {
	query := req.URL.Query()
	var platformQueueName buildqueuestate.PlatformQueueName
	if err := jsonpb.UnmarshalString(query.Get("platform_queue_name"), &platformQueueName); err != nil {
		http.Error(w, util.StatusWrap(err, "Failed to extract platform queue").Error(), http.StatusBadRequest)
		return
	}

	var startAfter *buildqueuestate.ListWorkersRequest_StartAfter
	if startAfterParameter := req.URL.Query().Get("start_after"); startAfterParameter != "" {
		var startAfterMessage buildqueuestate.ListWorkersRequest_StartAfter
		if err := jsonpb.UnmarshalString(startAfterParameter, &startAfterMessage); err != nil {
			http.Error(w, util.StatusWrap(err, "Failed to parse start after message").Error(), http.StatusBadRequest)
			return
		}
		startAfter = &startAfterMessage
	}
	justExecutingWorkers := query.Get("just_executing_workers") != ""

	ctx := req.Context()
	response, err := s.buildQueue.ListWorkers(ctx, &buildqueuestate.ListWorkersRequest{
		PlatformQueueName:    &platformQueueName,
		JustExecutingWorkers: justExecutingWorkers,
		PageSize:             pageSize,
		StartAfter:           startAfter,
	})
	if err != nil {
		// TODO: Pick the right error code.
		http.Error(w, util.StatusWrap(err, "Failed to list worker state").Error(), http.StatusBadRequest)
		return
	}

	var nextStartAfter *buildqueuestate.ListWorkersRequest_StartAfter
	if l := response.Workers; len(l) > 0 {
		w := l[len(l)-1]
		nextStartAfter = &buildqueuestate.ListWorkersRequest_StartAfter{
			WorkerId: w.Id,
		}
	}

	if err := listWorkerStateTemplate.Execute(w, struct {
		PlatformQueueName    *buildqueuestate.PlatformQueueName
		BrowserURL           *url.URL
		Now                  time.Time
		PaginationInfo       *buildqueuestate.PaginationInfo
		EndIndex             int
		StartAfter           *buildqueuestate.ListWorkersRequest_StartAfter
		Workers              []*buildqueuestate.WorkerState
		JustExecutingWorkers bool
	}{
		PlatformQueueName:    &platformQueueName,
		BrowserURL:           s.browserURL,
		Now:                  s.clock.Now(),
		PaginationInfo:       response.PaginationInfo,
		EndIndex:             int(response.PaginationInfo.StartIndex) + len(response.Workers),
		StartAfter:           nextStartAfter,
		Workers:              response.Workers,
		JustExecutingWorkers: justExecutingWorkers,
	}); err != nil {
		log.Print(err)
	}
}

func (s *buildQueueStateService) handleListDrains(w http.ResponseWriter, req *http.Request) {
	query := req.URL.Query()
	var platformQueueName buildqueuestate.PlatformQueueName
	if err := jsonpb.UnmarshalString(query.Get("platform_queue_name"), &platformQueueName); err != nil {
		http.Error(w, util.StatusWrap(err, "Failed to extract platform queue").Error(), http.StatusBadRequest)
		return
	}

	ctx := req.Context()
	response, err := s.buildQueue.ListDrains(ctx, &buildqueuestate.ListDrainsRequest{
		PlatformQueueName: &platformQueueName,
	})
	if err != nil {
		// TODO: Pick the right error code.
		http.Error(w, util.StatusWrap(err, "Failed to list drain state").Error(), http.StatusBadRequest)
		return
	}
	if err := listDrainStateTemplate.Execute(w, struct {
		PlatformQueueName *buildqueuestate.PlatformQueueName
		Now               time.Time
		Drains            []*buildqueuestate.DrainState
	}{
		PlatformQueueName: &platformQueueName,
		Now:               s.clock.Now(),
		Drains:            response.Drains,
	}); err != nil {
		log.Print(err)
	}
}

func handleModifyDrain(w http.ResponseWriter, req *http.Request, modifyFunc func(context.Context, *buildqueuestate.AddOrRemoveDrainRequest) (*empty.Empty, error)) {
	req.ParseForm()
	var platformQueueName buildqueuestate.PlatformQueueName
	if err := jsonpb.UnmarshalString(req.FormValue("platform_queue_name"), &platformQueueName); err != nil {
		http.Error(w, util.StatusWrap(err, "Failed to extract platform queue").Error(), http.StatusBadRequest)
		return
	}

	var workerIDPattern map[string]string
	if err := json.Unmarshal([]byte(req.FormValue("worker_id_pattern")), &workerIDPattern); err != nil {
		http.Error(w, util.StatusWrap(err, "Failed to extract worker ID pattern").Error(), http.StatusBadRequest)
		return
	}

	ctx := req.Context()
	if _, err := modifyFunc(ctx, &buildqueuestate.AddOrRemoveDrainRequest{
		PlatformQueueName: &platformQueueName,
		WorkerIdPattern:   workerIDPattern,
	}); err != nil {
		http.Error(w, util.StatusWrap(err, "Failed to modify drains").Error(), http.StatusBadRequest)
		return
	}
	http.Redirect(w, req, req.Header.Get("Referer"), http.StatusSeeOther)
}

func (s *buildQueueStateService) handleAddDrain(w http.ResponseWriter, req *http.Request) {
	handleModifyDrain(w, req, s.buildQueue.AddDrain)
}

func (s *buildQueueStateService) handleRemoveDrain(w http.ResponseWriter, req *http.Request) {
	handleModifyDrain(w, req, s.buildQueue.RemoveDrain)
}
