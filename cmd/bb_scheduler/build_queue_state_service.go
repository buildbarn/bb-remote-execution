package main

import (
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	re_util "github.com/buildbarn/bb-remote-execution/pkg/util"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/golang/protobuf/jsonpb"
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
	abbreviateFun           = func(s string) string {
		if len(s) > 11 {
			return s[:8] + "..."
		}
		return s
	}

	templateFuncMap = template.FuncMap{
		"abbreviate": abbreviateFun,
		"action_url": func(browserURL *url.URL, instanceName digest.InstanceName, actionDigest *remoteexecution.Digest) string {
			d, err := instanceName.NewDigestFromProto(actionDigest)
			if err != nil {
				return ""
			}
			return re_util.GetBrowserURL(browserURL, "action", d)
		},
		"last_element": func(s interface{}) interface{} {
			v := reflect.ValueOf(s)
			if l := v.Len(); l > 0 {
				return v.Index(l - 1).Interface()
			}
			return nil
		},
		"proto_to_json":          jsonpbMarshaler.MarshalToString,
		"proto_to_indented_json": jsonpbIndentedMarshaler.MarshalToString,
		"error_proto":            status.ErrorProto,
		"to_duration": func(large time.Time, small time.Time) string {
			return large.Sub(small).Truncate(time.Second).String()
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
		"to_timestamp": func(t time.Time) string {
			localLoc, _ := time.LoadLocation("Local")
			localDateTime := t.In(localLoc)
			return localDateTime.Format("15:04:05")
		},
		"present_if_value": func(format string, s *string) string {
			if s == nil {
				return ""
			}
			return fmt.Sprintf(format, abbreviateFun(*s))
		},
		"list_keywords": func(keyValueMap map[string]string) string {
			keys := make([]string, len(keyValueMap))

			i := 0
			for k := range keyValueMap {
				keys[i] = k
				i++
			}
			sort.Strings(keys)
			result := make([]string, i)
			for idx, key := range keys {
				result[idx] = fmt.Sprintf("%s=%s", key, keyValueMap[key])
			}
			return strings.Join(result, ", ")
		},
		"potential_disable": func(canceled bool) string {
			if canceled {
				return "disabled"
			}
			return ""
		},
		"operations_title": func(selection *string) string {
			if selection == nil {
				return "All"
			}
			return *selection
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
    <p>Total number of operations: <a href="operations">{{.State.OperationsCount}}</a></p>
    <h2>Platform queues</h2>
    <table>
      <thead>
        <tr>
          <th>Instance name</th>
          <th>Platform</th>
          <th>Timeout</th>
          <th>Invocations</th>
          <th>Queued operations</th>
          <th>Executing workers</th>
          <th>Drains</th>
        </tr>
      </thead>
      {{$now := .Now}}
      {{range .State.PlatformQueues}}
        {{$platform := proto_to_json .Platform}}
        <tr>
          <td>{{.InstanceName}}</td>
          <td>{{$platform}}</td>
          <td>{{with .Timeout}}{{to_duration . $now}}{{else}}∞{{end}}</td>
          <td><a href="active_invocations?instance_name={{.InstanceName}}">{{.InvocationCount}}</a></td>
          <td><a href="queued_operations?instance_name={{.InstanceName}}&amp;platform={{$platform}}">{{.QueuedOperationsCount}}</a></td>
          <td>
            <a href="workers?instance_name={{.InstanceName}}&amp;platform={{$platform}}&amp;just_executing_workers=true">{{.ExecutingWorkersCount}}</a>
            /
            <a href="workers?instance_name={{.InstanceName}}&amp;platform={{$platform}}">{{.WorkersCount}}</a>
          </td>
          <td>
            <a href="drains?instance_name={{.InstanceName}}&amp;platform={{$platform}}">{{.DrainsCount}}</a>
          </td>
        </tr>
      {{end}}
    </table>
  </body>
</html>
`))
	getDetailedOperationStateTemplate = template.Must(template.New("GetDetailedOperationState").Funcs(templateFuncMap).Parse(`
<!DOCTYPE html>
<html>
  <head>
    <title>Operation {{.DetailedOperation.Name}}</title>
    <style>
      html { font-family: sans-serif; }
    </style>
  </head>
  <body>
    <h1>Operation {{.DetailedOperation.Name}}</h1>
    {{$now := .Now}}
    <p>Age: {{to_duration $now .DetailedOperation.QueuedTimestamp}}<br/>
    Timeout: {{with .DetailedOperation.Timeout}}{{to_duration . $now}}{{else}}∞{{end}}<br/>
    Instance name: {{.DetailedOperation.InstanceName}}<br/>
    Action digest: <a href="{{action_url .BrowserURL .DetailedOperation.InstanceName .DetailedOperation.ActionDigest}}">{{proto_to_json .DetailedOperation.ActionDigest}}</a><br/>
    Argv[0]: {{.DetailedOperation.Argv0}}<br/>
    Stage:
      {{with .DetailedOperation.ExecuteResponse}}
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
        {{if eq .DetailedOperation.Stage 2}}
          Queued
        {{else}}
          {{if eq .DetailedOperation.Stage 3}}
            Executing
          {{else}}
            Unknown
          {{end}}
        {{end}}
      {{end}}
    </p>
    {{with .DetailedOperation.ExecuteResponse}}
      <h2>Execute response</h2>
      <pre>{{proto_to_indented_json .}}</pre>
    {{else}}
      <form action="kill_operation" method="post">
        <p>
          <input name="name" type="hidden" value="{{.DetailedOperation.Name}}"/>
          <input type="submit" value="Kill operation"/>
        </p>
      </form>
    {{end}}
  </body>
</html>
`))
	listDetailedOperationStateTemplate = template.Must(template.New("ListDetailedOperationState").Funcs(templateFuncMap).Parse(`
<!DOCTYPE html>
<html>
  <head>
    <title>{{operations_title .Selection}} operations {{present_if_value "for invocationID %s" .InvocationID}}</title>
    <style>
      html { font-family: sans-serif; }
      table { border-collapse: collapse; }
      table, td, th { border: 1px solid black; }
      td, th { padding-left: 5px; padding-right: 5px; }
      .text-monospace { font-family: monospace; }
    </style>
  </head>
  <body>
    <h1>{{operations_title .Selection}} operations {{present_if_value "for invocationID %s" .InvocationID}}</h1>
    <p>Showing operations [{{.PaginationInfo.StartIndex}}, {{.PaginationInfo.EndIndex}}) of {{.PaginationInfo.TotalEntries}} in total.
      {{with last_element .DetailedOperations}}
        <a href="?start_after_operation={{.Name}}">&gt;&gt;&gt;</a>
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
      {{range .DetailedOperations}}
        <tr>
          <td>{{with .Timeout}}{{to_duration . $now}}{{else}}∞{{end}}</td>
          <td style="background-color: {{to_background_color .Name}}">
            <a class="text-monospace" href="operation?name={{.Name}}" style="color: {{to_foreground_color .Name}}">{{abbreviate .Name}}</a>
          </td>
          <td style="background-color: {{to_background_color .ActionDigest.Hash}}">
            <a class="text-monospace" href="{{action_url $browserURL .InstanceName .ActionDigest}}" style="color: {{to_foreground_color .ActionDigest.Hash}}">{{abbreviate .ActionDigest.Hash}}</a>
          </td>
          <td>{{.Argv0}}</td>
          {{with .ExecuteResponse}}
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
            {{if eq .Stage 2}}
              <td>Queued</td>
            {{else}}
              {{if eq .Stage 3}}
                <td style="background-color: lightblue">Executing</td>
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
	listActiveInvocationsTemplate = template.Must(template.New("ListActiveInvocations").Funcs(templateFuncMap).Parse(`
<!DOCTYPE html>
<html>
  <head>
    <title>Active bazel invocations</title>
    <style>
      html { font-family: sans-serif; }
      table { border-collapse: collapse; }
      table, td, th { border: 1px solid black; }
      td, th { padding-left: 5px; padding-right: 5px; }
      .text-monospace { font-family: monospace; }
    </style>
  </head>
  <body>
    {{$instanceName := .InstanceName}}
    <h1>Recent invocations</h1>
    <p>Instance name: {{$instanceName}}</p>
    <p><a href="inactive_invocations?instance_name={{.InstanceName}}">Show inactive invocations</a></p>
    <table>
      <thead>
        <tr>
          <th>Invocation<br/>ID</th>
          <th>First<br/>operation</th>
          <th>Priority</th>
          <th>Queued<br/>operations</th>
          <th>Executing<br/>operations</th>
          <th>Finished<br/>recent<br/>operations</th>
          <th>Keywords</th>
          <th>Control</th>
          <th>Timeout</br>
        </tr>
      </thead>
      {{$browserURL := .BrowserURL}}
      {{$now := .Now}}
      {{range .Invocations}}
        <tr>
          <td><a href="operations?invocation_id={{.InvocationID}}">{{abbreviate .InvocationID}}</a></td>
          <td>{{to_timestamp .InvocationTimestamp}}</td>
          <td>{{.Priority}}</td>
          <td><a href="operations?invocation_id={{.InvocationID}}&selection=Queued">{{.QueuedOperationsCount}}</a></td>
          <td><a href="operations?invocation_id={{.InvocationID}}&selection=Executing">{{.ExecutingOperationsCount}}</a></td>
          <td><a href="operations?invocation_id={{.InvocationID}}&selection=Finished">{{.FinishedOperationsCount}}</a></td>
          <td>{{list_keywords .Keywords}}</td>
          <td> <button {{potential_disable .Canceled}} onclick="window.location.href='kill_invocation?name={{.InvocationID}}';" value="">Cancel</button></td>
          <td>{{with .InvocationTimeout}}{{to_duration . $now}}{{else}}∞{{end}}</td>
        </tr>
      {{end}}
    </table>
  </body>
</html>
`))
	listInactiveInvocationsTemplate = template.Must(template.New("ListInactiveInvocations").Funcs(templateFuncMap).Parse(`
<!DOCTYPE html>
<html>
  <head>
    <title>Inactive bazel invocations</title>
    <style>
      html { font-family: sans-serif; }
      table { border-collapse: collapse; }
      table, td, th { border: 1px solid black; }
      td, th { padding-left: 5px; padding-right: 5px; }
      .text-monospace { font-family: monospace; }
    </style>
  </head>
  <body>
    {{$instanceName := .InstanceName}}
    <h1>Inactive invocations</h1>
    <p>Instance name: {{$instanceName}}</p>
    <p><a href="active_invocations?instance_name={{.InstanceName}}">Show active invocations</a></p>
    <table>
      <thead>
        <tr>
          <th>Invocation<br/>ID</th>
          <th>First<br/>operation</th>
          <th>Priority</th>
          <th>Keywords</th>
          <th>Control</th>
          <th>Timeout</br>
        </tr>
      </thead>
      {{$browserURL := .BrowserURL}}
      {{$now := .Now}}
      {{range .Invocations}}
        <tr>
          <td><a href="operations?invocation_id={{.InvocationID}}">{{abbreviate .InvocationID}}</a></td>
          <td>{{to_timestamp .InvocationTimestamp}}</td>
          <td>{{.Priority}}</td>
          <td>{{list_keywords .Keywords}}</td>
          <td> <button {{potential_disable .Canceled}} onclick="window.location.href='kill_invocation?name={{.InvocationID}}';" value="">Cancel</button></td>
          <td>{{with .InvocationTimeout}}{{to_duration . $now}}{{else}}∞{{end}}</td>
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
    {{$instanceName := .InstanceName}}
    <p>Instance name: {{$instanceName}}<br/>
    {{$platform := proto_to_json .Platform}}
    Platform: {{$platform}}</p>
    <p>Showing queued operations [{{.PaginationInfo.StartIndex}}, {{.PaginationInfo.EndIndex}}) of {{.PaginationInfo.TotalEntries}} in total.
      {{with last_element .QueuedOperations}}
        <a href="?instance_name={{$instanceName}}&amp;platform={{$platform}}&amp;start_after_priority={{.Priority}}&amp;start_after_queued_timestamp={{to_json .QueuedTimestamp}}">&gt;&gt;&gt;</a>
      {{end}}
    </p>
    <table>
      <thead>
        <tr>
          <th>Priority</th>
          <th>Invocation ID</th>
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
          <td>{{.Priority}}</td>
          <td>{{abbreviate .InvocationID}}</a></td>
          <td>{{to_duration $now .QueuedTimestamp}}</td>
          <td>{{with .Timeout}}{{to_duration . $now}}{{else}}∞{{end}}</td>
          <td style="background-color: {{to_background_color .Name}}">
            <a class="text-monospace" href="operation?name={{.Name}}" style="color: {{to_foreground_color .Name}}">{{abbreviate .Name}}</a>
          </td>
          <td style="background-color: {{to_background_color .ActionDigest.Hash}}">
            <a class="text-monospace" href="{{action_url $browserURL $instanceName .ActionDigest}}" style="color: {{to_foreground_color .ActionDigest.Hash}}">{{abbreviate .ActionDigest.Hash}}</a>
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
    {{$instanceName := .InstanceName}}
    {{$platform := proto_to_json .Platform}}
    <p>Instance name: {{$instanceName}}<br/>
    Platform: {{$platform}}</p>
    <p>Showing workers [{{.PaginationInfo.StartIndex}}, {{.PaginationInfo.EndIndex}}) of {{.PaginationInfo.TotalEntries}} in total.
      {{with last_element .Workers}}
        <a href="?instance_name={{$instanceName}}&amp;platform={{$platform}}&amp;start_after_worker_id={{to_json .WorkerID}}">&gt;&gt;&gt;</a>
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
        {{$workerID := to_json .WorkerID}}
        <tr>
          <td>{{$workerID}}</td>
          <td>{{with .Timeout}}{{to_duration . $now}}{{else}}∞{{end}}</td>
          {{with .CurrentOperation}}
            <td>{{with .Timeout}}{{to_duration . $now}}{{else}}∞{{end}}</td>
            <td style="background-color: {{to_background_color .Name}}">
              <a class="text-monospace" href="operation?name={{.Name}}" style="color: {{to_foreground_color .Name}}">{{abbreviate .Name}}</a>
            </td>
            <td style="background-color: {{to_background_color .ActionDigest.Hash}}">
              <a class="text-monospace" href="{{action_url $browserURL $instanceName .ActionDigest}}" style="color: {{to_foreground_color .ActionDigest.Hash}}">{{abbreviate .ActionDigest.Hash}}</a>
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
    {{$instanceName := .InstanceName}}
    {{$platform := proto_to_json .Platform}}
    <p>Instance name: {{$instanceName}}<br/>
    Platform: {{$platform}}</p>
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
        {{$workerIDPattern := to_json .WorkerIDPattern}}
        <tr>
          <td>{{$workerIDPattern}}</td>
          <td>{{to_duration $now .CreationTimestamp}}</td>
          <td>
            <form action="remove_drain" method="post">
              <input name="instance_name" type="hidden" value="{{$instanceName}}"/>
              <input name="platform" type="hidden" value="{{$platform}}"/>
              <input name="worker_id_pattern" type="hidden" value="{{$workerIDPattern}}"/>
              <input type="submit" value="Remove"/>
            </form>
          </td>
        </tr>
      {{end}}
    </table>
    <form action="add_drain" method="post">
      <p>
        <input name="instance_name" type="hidden" value="{{$instanceName}}"/>
        <input name="platform" type="hidden" value="{{$platform}}"/>
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
	buildQueue builder.BuildQueueStateProvider
	clock      clock.Clock
	browserURL *url.URL
}

func newBuildQueueStateService(buildQueue builder.BuildQueueStateProvider, clock clock.Clock, browserURL *url.URL, router *mux.Router) *buildQueueStateService {
	s := &buildQueueStateService{
		buildQueue: buildQueue,
		clock:      clock,
		browserURL: browserURL,
	}
	router.HandleFunc("/", s.handleGetBuildQueueState)
	router.HandleFunc("/add_drain", s.handleAddDrain)
	router.HandleFunc("/drains", s.handleListDrainState)
	router.HandleFunc("/active_invocations", s.handleListActiveInvocations)
	router.HandleFunc("/inactive_invocations", s.handleListInactiveInvocations)
	router.HandleFunc("/kill_operation", s.handleKillOperation)
	router.HandleFunc("/kill_invocation", s.handleKillInvocation)
	router.HandleFunc("/operation", s.handleGetDetailedOperationState)
	router.HandleFunc("/operations", s.handleListDetailedOperationState)
	router.HandleFunc("/queued_operations", s.handleListQueuedOperationState)
	router.HandleFunc("/remove_drain", s.handleRemoveDrain)
	router.HandleFunc("/workers", s.handleListWorkerState)
	return s
}

func (s *buildQueueStateService) handleGetBuildQueueState(w http.ResponseWriter, req *http.Request) {
	if err := getBuildQueueStateTemplate.Execute(w, struct {
		Now   time.Time
		State *builder.BuildQueueState
	}{
		Now:   s.clock.Now(),
		State: s.buildQueue.GetBuildQueueState(),
	}); err != nil {
		log.Print(err)
	}
}

func (s *buildQueueStateService) handleKillOperation(w http.ResponseWriter, req *http.Request) {
	req.ParseForm()
	if !s.buildQueue.KillOperation(req.FormValue("name")) {
		http.Error(w, "Operation not found", http.StatusNotFound)
		return
	}
	http.Redirect(w, req, req.Header.Get("Referer"), http.StatusSeeOther)
}

func (s *buildQueueStateService) handleKillInvocation(w http.ResponseWriter, req *http.Request) {
	req.ParseForm()
	if !s.buildQueue.KillInvocation(req.FormValue("name")) {
		http.Error(w, "Invocation not found: "+req.FormValue("name"), http.StatusNotFound)
		return
	}
	http.Redirect(w, req, req.Header.Get("Referer"), http.StatusSeeOther)
}

func (s *buildQueueStateService) handleGetDetailedOperationState(w http.ResponseWriter, req *http.Request) {
	detailedOperation, ok := s.buildQueue.GetDetailedOperationState(req.URL.Query().Get("name"))
	if !ok {
		http.Error(w, "Operation not found", http.StatusNotFound)
		return
	}
	if err := getDetailedOperationStateTemplate.Execute(w, struct {
		BrowserURL        *url.URL
		Now               time.Time
		DetailedOperation *builder.DetailedOperationState
	}{
		BrowserURL:        s.browserURL,
		Now:               s.clock.Now(),
		DetailedOperation: detailedOperation,
	}); err != nil {
		log.Print(err)
	}
}

func (s *buildQueueStateService) handleListDetailedOperationState(w http.ResponseWriter, req *http.Request) {
	var startAfterOperation *string
	if operationParameter := req.URL.Query().Get("start_after_operation"); operationParameter != "" {
		startAfterOperation = &operationParameter
	}
	var invocationID *string
	if operationParameter := req.URL.Query().Get("invocation_id"); operationParameter != "" {
		invocationID = &operationParameter
	}
	var selection *string
	if operationParameter := req.URL.Query().Get("selection"); operationParameter != "" {
		selection = &operationParameter
	}

	detailedOperations, paginationInfo, err := s.buildQueue.ListDetailedOperationState(invocationID, selection, pageSize, startAfterOperation)
	if err != nil {
		http.Error(w, util.StatusWrap(err, "Failed to list operations").Error(), http.StatusBadRequest)
		return
	}
	if err := listDetailedOperationStateTemplate.Execute(w, struct {
		BrowserURL         *url.URL
		Now                time.Time
		InvocationID       *string
		Selection          *string
		PaginationInfo     builder.PaginationInfo
		DetailedOperations []builder.DetailedOperationState
	}{
		BrowserURL:         s.browserURL,
		Now:                s.clock.Now(),
		InvocationID:       invocationID,
		Selection:          selection,
		PaginationInfo:     paginationInfo,
		DetailedOperations: detailedOperations,
	}); err != nil {
		log.Print(err)
	}
}

func (s *buildQueueStateService) doHandleListInvocations(w http.ResponseWriter, instanceName digest.InstanceName, active bool) {
	invocations, paginationInfo, err := s.buildQueue.ListInvocations(instanceName, pageSize, active)
	if err != nil {
		http.Error(w, util.StatusWrap(err, "Failed to list invocations").Error(), http.StatusBadRequest)
		return
	}
	sort.Slice(invocations, func(i, j int) bool {
		return invocations[i].InvocationTimestamp.Before(invocations[j].InvocationTimestamp)
	})
	template := listActiveInvocationsTemplate
	if !active {
		template = listInactiveInvocationsTemplate
	}
	if err := template.Execute(w, struct {
		InstanceName   digest.InstanceName
		BrowserURL     *url.URL
		Now            time.Time
		PaginationInfo builder.PaginationInfo
		Invocations    []builder.InvocationEntry
	}{
		InstanceName:   instanceName,
		BrowserURL:     s.browserURL,
		Now:            s.clock.Now(),
		PaginationInfo: paginationInfo,
		Invocations:    invocations,
	}); err != nil {
		log.Print(err)
	}
}

func (s *buildQueueStateService) handleListActiveInvocations(w http.ResponseWriter, req *http.Request) {
	query := req.URL.Query()
	instanceName, err := digest.NewInstanceName(query.Get("instance_name"))
	if err != nil {
		http.Error(w, util.StatusWrapf(err, "Invalid instance name %#v", query.Get("instance_name")).Error(), http.StatusBadRequest)
		return
	}
	s.doHandleListInvocations(w, instanceName, true)
}

func (s *buildQueueStateService) handleListInactiveInvocations(w http.ResponseWriter, req *http.Request) {
	query := req.URL.Query()
	instanceName, err := digest.NewInstanceName(query.Get("instance_name"))
	if err != nil {
		http.Error(w, util.StatusWrapf(err, "Invalid instance name %#v", query.Get("instance_name")).Error(), http.StatusBadRequest)
		return
	}
	s.doHandleListInvocations(w, instanceName, false)
}

func (s *buildQueueStateService) handleListQueuedOperationState(w http.ResponseWriter, req *http.Request) {
	query := req.URL.Query()
	instanceName, err := digest.NewInstanceName(query.Get("instance_name"))
	if err != nil {
		http.Error(w, util.StatusWrapf(err, "Invalid instance name %#v", query.Get("instance_name")).Error(), http.StatusBadRequest)
		return
	}
	var platform remoteexecution.Platform
	if err := jsonpb.UnmarshalString(query.Get("platform"), &platform); err != nil {
		http.Error(w, util.StatusWrap(err, "Failed to extract platform").Error(), http.StatusBadRequest)
		return
	}
	var startAfterPriority *int32
	if priorityParameter := query.Get("start_after_priority"); priorityParameter != "" {
		i, err := strconv.ParseInt(priorityParameter, 10, 32)
		if err != nil {
			http.Error(w, util.StatusWrap(err, "Failed to extract priority offset").Error(), http.StatusBadRequest)
			return
		}
		priority := int32(i)
		startAfterPriority = &priority
	}
	var startAfterQueuedTimestamp *time.Time
	if queuedTimestampParameter := query.Get("start_after_queued_timestamp"); queuedTimestampParameter != "" {
		var queuedTimestamp time.Time
		if err := queuedTimestamp.UnmarshalJSON([]byte(queuedTimestampParameter)); err != nil {
			http.Error(w, util.StatusWrap(err, "Failed to extract timestamp offset").Error(), http.StatusBadRequest)
			return
		}
		startAfterQueuedTimestamp = &queuedTimestamp
	}

	queuedOperations, paginationInfo, err := s.buildQueue.ListQueuedOperationState(instanceName, &platform, pageSize, startAfterPriority, startAfterQueuedTimestamp)
	if err != nil {
		// TODO: Pick the right error code.
		http.Error(w, util.StatusWrap(err, "Failed to list queued operation state").Error(), http.StatusBadRequest)
		return
	}
	if err := listQueuedOperationStateTemplate.Execute(w, struct {
		InstanceName     digest.InstanceName
		Platform         *remoteexecution.Platform
		BrowserURL       *url.URL
		Now              time.Time
		PaginationInfo   builder.PaginationInfo
		QueuedOperations []builder.QueuedOperationState
	}{
		InstanceName:     instanceName,
		Platform:         &platform,
		BrowserURL:       s.browserURL,
		Now:              s.clock.Now(),
		PaginationInfo:   paginationInfo,
		QueuedOperations: queuedOperations,
	}); err != nil {
		log.Print(err)
	}
}

func (s *buildQueueStateService) handleListWorkerState(w http.ResponseWriter, req *http.Request) {
	query := req.URL.Query()
	instanceName, err := digest.NewInstanceName(query.Get("instance_name"))
	if err != nil {
		http.Error(w, util.StatusWrapf(err, "Invalid instance name %#v", query.Get("instance_name")).Error(), http.StatusBadRequest)
		return
	}
	var platform remoteexecution.Platform
	if err := jsonpb.UnmarshalString(query.Get("platform"), &platform); err != nil {
		http.Error(w, util.StatusWrap(err, "Failed to extract platform").Error(), http.StatusBadRequest)
		return
	}
	var startAfterWorkerID map[string]string
	if workerIDParameter := query.Get("start_after_worker_id"); workerIDParameter != "" {
		if err := json.Unmarshal([]byte(workerIDParameter), &startAfterWorkerID); err != nil {
			http.Error(w, util.StatusWrap(err, "Failed to extract worker ID offset").Error(), http.StatusBadRequest)
			return
		}
	}
	justExecutingWorkers := query.Get("just_executing_workers") != ""

	workers, paginationInfo, err := s.buildQueue.ListWorkerState(instanceName, &platform, justExecutingWorkers, pageSize, startAfterWorkerID)
	if err != nil {
		// TODO: Pick the right error code.
		http.Error(w, util.StatusWrap(err, "Failed to list worker state").Error(), http.StatusBadRequest)
		return
	}
	if err := listWorkerStateTemplate.Execute(w, struct {
		InstanceName         digest.InstanceName
		Platform             *remoteexecution.Platform
		BrowserURL           *url.URL
		Now                  time.Time
		PaginationInfo       builder.PaginationInfo
		Workers              []builder.WorkerState
		JustExecutingWorkers bool
	}{
		InstanceName:         instanceName,
		Platform:             &platform,
		BrowserURL:           s.browserURL,
		Now:                  s.clock.Now(),
		PaginationInfo:       paginationInfo,
		Workers:              workers,
		JustExecutingWorkers: justExecutingWorkers,
	}); err != nil {
		log.Print(err)
	}
}

func (s *buildQueueStateService) handleListDrainState(w http.ResponseWriter, req *http.Request) {
	query := req.URL.Query()
	instanceName, err := digest.NewInstanceName(query.Get("instance_name"))
	if err != nil {
		http.Error(w, util.StatusWrapf(err, "Invalid instance name %#v", query.Get("instance_name")).Error(), http.StatusBadRequest)
		return
	}
	var platform remoteexecution.Platform
	if err := jsonpb.UnmarshalString(query.Get("platform"), &platform); err != nil {
		http.Error(w, util.StatusWrap(err, "Failed to extract platform").Error(), http.StatusBadRequest)
		return
	}

	drains, err := s.buildQueue.ListDrainState(instanceName, &platform)
	if err != nil {
		// TODO: Pick the right error code.
		http.Error(w, util.StatusWrap(err, "Failed to list drain state").Error(), http.StatusBadRequest)
		return
	}
	if err := listDrainStateTemplate.Execute(w, struct {
		InstanceName digest.InstanceName
		Platform     *remoteexecution.Platform
		Now          time.Time
		Drains       []builder.DrainState
	}{
		InstanceName: instanceName,
		Platform:     &platform,
		Now:          s.clock.Now(),
		Drains:       drains,
	}); err != nil {
		log.Print(err)
	}
}

func handleModifyDrain(w http.ResponseWriter, req *http.Request, modifyFunc func(digest.InstanceName, *remoteexecution.Platform, map[string]string) error) {
	req.ParseForm()
	instanceName, err := digest.NewInstanceName(req.FormValue("instance_name"))
	if err != nil {
		http.Error(w, util.StatusWrapf(err, "Invalid instance name %#v", req.FormValue("instance_name")).Error(), http.StatusBadRequest)
		return
	}
	var platform remoteexecution.Platform
	if err := jsonpb.UnmarshalString(req.FormValue("platform"), &platform); err != nil {
		http.Error(w, util.StatusWrap(err, "Failed to extract platform").Error(), http.StatusBadRequest)
		return
	}

	var workerIDPattern map[string]string
	if err := json.Unmarshal([]byte(req.FormValue("worker_id_pattern")), &workerIDPattern); err != nil {
		http.Error(w, util.StatusWrap(err, "Failed to extract worker ID pattern").Error(), http.StatusBadRequest)
		return
	}

	if err := modifyFunc(instanceName, &platform, workerIDPattern); err != nil {
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
