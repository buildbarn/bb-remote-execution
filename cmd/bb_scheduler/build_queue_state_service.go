package main

import (
	"context"
	"embed"
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
	bb_http "github.com/buildbarn/bb-storage/pkg/http"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/gorilla/mux"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// pageSize controls how many elements are showed in pages
	// containing listings of workers, operations, etc. These pages
	// can become quite big for large setups.
	pageSize = 1000
)

var (
	//go:embed templates
	templatesFS embed.FS
	//go:embed stylesheet.css
	stylesheet template.CSS

	templates = template.Must(template.New("templates").Funcs(template.FuncMap{
		"abbreviate": func(s string) string {
			if len(s) > 11 {
				return s[:8] + "..."
			}
			return s
		},
		"action_url": func(browserURL *url.URL, instanceNamePrefix, instanceNameSuffix string, actionDigest *remoteexecution.Digest) string {
			iPrefix, err := digest.NewInstanceName(instanceNamePrefix)
			if err != nil {
				return ""
			}
			iSuffix, err := digest.NewInstanceName(instanceNameSuffix)
			if err != nil {
				return ""
			}
			instanceName := digest.NewInstanceNamePatcher(digest.EmptyInstanceName, iPrefix).PatchInstanceName(iSuffix)
			d, err := instanceName.NewDigestFromProto(actionDigest)
			if err != nil {
				return ""
			}
			return re_util.GetBrowserURL(browserURL, "action", d)
		},
		"get_child_invocation_name": func(parent *buildqueuestate.InvocationName, id *anypb.Any) *buildqueuestate.InvocationName {
			return &buildqueuestate.InvocationName{
				SizeClassQueueName: parent.SizeClassQueueName,
				Ids:                append(append(make([]*anypb.Any, 0, len(parent.Ids)+1), parent.Ids...), id),
			}
		},
		"get_size_class_queue_name": func(platformQueueName *buildqueuestate.PlatformQueueName, sizeClass uint32) *buildqueuestate.SizeClassQueueName {
			return &buildqueuestate.SizeClassQueueName{
				PlatformQueueName: platformQueueName,
				SizeClass:         sizeClass,
			}
		},
		"operation_stage_queued": func(o *buildqueuestate.OperationState) *emptypb.Empty {
			if s, ok := o.Stage.(*buildqueuestate.OperationState_Queued); ok {
				return s.Queued
			}
			return nil
		},
		"operation_stage_executing": func(o *buildqueuestate.OperationState) *emptypb.Empty {
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
		"proto_to_json":           protojson.MarshalOptions{}.Format,
		"proto_to_json_multiline": protojson.MarshalOptions{Multiline: true}.Format,
		"error_proto":             status.ErrorProto,
		"stylesheet":              func() template.CSS { return stylesheet },
		"time_future": func(t *timestamppb.Timestamp, now time.Time) string {
			if t == nil {
				return "∞"
			}
			if t.CheckValid() != nil {
				return "?"
			}
			return t.AsTime().Sub(now).Truncate(time.Second).String()
		},
		"time_past": func(t *timestamppb.Timestamp, now time.Time) string {
			if t.CheckValid() != nil {
				return "?"
			}
			return now.Sub(t.AsTime()).Truncate(time.Second).String()
		},
		"to_json": func(v interface{}) (string, error) {
			b, err := json.MarshalIndent(v, " ", "")
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
	}).ParseFS(templatesFS, "templates/*.html"))
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

func renderError(w http.ResponseWriter, err error) {
	s := status.Convert(err)
	w.WriteHeader(bb_http.StatusCodeFromGRPCCode(s.Code()))
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if err := templates.ExecuteTemplate(w, "error.html", s); err != nil {
		log.Print(err)
	}
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
	router.HandleFunc("/invocation_children", s.handleListInvocationChildren)
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
		renderError(w, util.StatusWrap(err, "Failed to list platform queues"))
		return
	}
	response, err := s.buildQueue.ListPlatformQueues(ctx, &emptypb.Empty{})
	if err != nil {
		renderError(w, util.StatusWrap(err, "Failed to list platform queues"))
		return
	}

	if err := templates.ExecuteTemplate(w, "get_build_queue_state.html", struct {
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

func (s *buildQueueStateService) handleListInvocationChildren(w http.ResponseWriter, req *http.Request) {
	query := req.URL.Query()
	var invocationName buildqueuestate.InvocationName
	if err := protojson.Unmarshal([]byte(query.Get("invocation_name")), &invocationName); err != nil {
		renderError(w, util.StatusWrapWithCode(err, codes.InvalidArgument, "Failed to extract invocation name"))
		return
	}
	filterString := query.Get("filter")
	filterValue, ok := buildqueuestate.ListInvocationChildrenRequest_Filter_value[filterString]
	if !ok {
		renderError(w, status.Error(codes.InvalidArgument, "Invalid filter"))
		return
	}

	ctx := req.Context()
	response, err := s.buildQueue.ListInvocationChildren(ctx, &buildqueuestate.ListInvocationChildrenRequest{
		InvocationName: &invocationName,
		Filter:         buildqueuestate.ListInvocationChildrenRequest_Filter(filterValue),
	})
	if err != nil {
		renderError(w, util.StatusWrap(err, "Failed to list invocation children"))
		return
	}
	if err := templates.ExecuteTemplate(w, "list_invocation_child_state.html", struct {
		InvocationName *buildqueuestate.InvocationName
		Children       []*buildqueuestate.InvocationChildState
		Filter         string
		Now            time.Time
	}{
		InvocationName: &invocationName,
		Children:       response.Children,
		Filter:         filterString,
		Now:            s.clock.Now(),
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
		renderError(w, util.StatusWrap(err, "Failed to kill operation"))
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
		renderError(w, util.StatusWrap(err, "Failed to get operation"))
		return
	}
	if err := templates.ExecuteTemplate(w, "get_operation_state.html", struct {
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
	query := req.URL.Query()
	var filterInvocationID *anypb.Any
	if filterInvocationIDString := query.Get("filter_invocation_id"); filterInvocationIDString != "" {
		var invocationID anypb.Any
		if err := protojson.Unmarshal([]byte(filterInvocationIDString), &invocationID); err != nil {
			renderError(w, status.Error(codes.InvalidArgument, "Invalid filter invocation ID"))
			return
		}
		filterInvocationID = &invocationID
	}

	filterStageString := query.Get("filter_stage")
	filterStageValue, ok := remoteexecution.ExecutionStage_Value_value[filterStageString]
	if !ok {
		renderError(w, status.Error(codes.InvalidArgument, "Invalid filter stage"))
		return
	}

	var startAfter *buildqueuestate.ListOperationsRequest_StartAfter
	if startAfterParameter := query.Get("start_after"); startAfterParameter != "" {
		var startAfterMessage buildqueuestate.ListOperationsRequest_StartAfter
		if err := protojson.Unmarshal([]byte(startAfterParameter), &startAfterMessage); err != nil {
			renderError(w, util.StatusWrapWithCode(err, codes.InvalidArgument, "Failed to parse start after message"))
			return
		}
		startAfter = &startAfterMessage
	}

	ctx := req.Context()
	response, err := s.buildQueue.ListOperations(ctx, &buildqueuestate.ListOperationsRequest{
		FilterInvocationId: filterInvocationID,
		FilterStage:        remoteexecution.ExecutionStage_Value(filterStageValue),
		PageSize:           pageSize,
		StartAfter:         startAfter,
	})
	if err != nil {
		renderError(w, util.StatusWrap(err, "Failed to list operations"))
		return
	}

	var nextStartAfter *buildqueuestate.ListOperationsRequest_StartAfter
	if l := response.Operations; len(l) > 0 {
		o := l[len(l)-1]
		nextStartAfter = &buildqueuestate.ListOperationsRequest_StartAfter{
			OperationName: o.Name,
		}
	}

	if err := templates.ExecuteTemplate(w, "list_operation_state.html", struct {
		BrowserURL         *url.URL
		Now                time.Time
		PaginationInfo     *buildqueuestate.PaginationInfo
		EndIndex           int
		FilterInvocationID *anypb.Any
		FilterStage        string
		StartAfter         *buildqueuestate.ListOperationsRequest_StartAfter
		Operations         []*buildqueuestate.OperationState
	}{
		BrowserURL:         s.browserURL,
		Now:                s.clock.Now(),
		PaginationInfo:     response.PaginationInfo,
		EndIndex:           int(response.PaginationInfo.StartIndex) + len(response.Operations),
		FilterInvocationID: filterInvocationID,
		FilterStage:        filterStageString,
		StartAfter:         nextStartAfter,
		Operations:         response.Operations,
	}); err != nil {
		log.Print(err)
	}
}

func (s *buildQueueStateService) handleListQueuedOperations(w http.ResponseWriter, req *http.Request) {
	query := req.URL.Query()
	var invocationName buildqueuestate.InvocationName
	if err := protojson.Unmarshal([]byte(query.Get("invocation_name")), &invocationName); err != nil {
		renderError(w, util.StatusWrapWithCode(err, codes.InvalidArgument, "Failed to extract invocation name"))
		return
	}

	var startAfter *buildqueuestate.ListQueuedOperationsRequest_StartAfter
	if startAfterParameter := req.URL.Query().Get("start_after"); startAfterParameter != "" {
		var startAfterMessage buildqueuestate.ListQueuedOperationsRequest_StartAfter
		if err := protojson.Unmarshal([]byte(startAfterParameter), &startAfterMessage); err != nil {
			renderError(w, util.StatusWrapWithCode(err, codes.InvalidArgument, "Failed to parse start after message"))
			return
		}
		startAfter = &startAfterMessage
	}

	ctx := req.Context()
	response, err := s.buildQueue.ListQueuedOperations(ctx, &buildqueuestate.ListQueuedOperationsRequest{
		InvocationName: &invocationName,
		PageSize:       pageSize,
		StartAfter:     startAfter,
	})
	if err != nil {
		renderError(w, util.StatusWrap(err, "Failed to list queued operation state"))
		return
	}

	var nextStartAfter *buildqueuestate.ListQueuedOperationsRequest_StartAfter
	if l := response.QueuedOperations; len(l) > 0 {
		o := l[len(l)-1]
		nextStartAfter = &buildqueuestate.ListQueuedOperationsRequest_StartAfter{
			Priority:        o.Priority,
			QueuedTimestamp: o.QueuedTimestamp,
		}
	}

	if err := templates.ExecuteTemplate(w, "list_queued_operation_state.html", struct {
		InvocationName   *buildqueuestate.InvocationName
		BrowserURL       *url.URL
		Now              time.Time
		PaginationInfo   *buildqueuestate.PaginationInfo
		EndIndex         int
		StartAfter       *buildqueuestate.ListQueuedOperationsRequest_StartAfter
		QueuedOperations []*buildqueuestate.OperationState
	}{
		InvocationName:   &invocationName,
		BrowserURL:       s.browserURL,
		Now:              s.clock.Now(),
		PaginationInfo:   response.PaginationInfo,
		EndIndex:         int(response.PaginationInfo.StartIndex) + len(response.QueuedOperations),
		StartAfter:       nextStartAfter,
		QueuedOperations: response.QueuedOperations,
	}); err != nil {
		log.Print(err)
	}
}

func (s *buildQueueStateService) handleListWorkers(w http.ResponseWriter, req *http.Request) {
	query := req.URL.Query()
	var filter buildqueuestate.ListWorkersRequest_Filter
	if err := protojson.Unmarshal([]byte(query.Get("filter")), &filter); err != nil {
		renderError(w, util.StatusWrapWithCode(err, codes.InvalidArgument, "Failed to extract filter"))
		return
	}

	var startAfter *buildqueuestate.ListWorkersRequest_StartAfter
	if startAfterParameter := req.URL.Query().Get("start_after"); startAfterParameter != "" {
		var startAfterMessage buildqueuestate.ListWorkersRequest_StartAfter
		if err := protojson.Unmarshal([]byte(startAfterParameter), &startAfterMessage); err != nil {
			renderError(w, util.StatusWrapWithCode(err, codes.InvalidArgument, "Failed to parse start after message"))
			return
		}
		startAfter = &startAfterMessage
	}

	ctx := req.Context()
	response, err := s.buildQueue.ListWorkers(ctx, &buildqueuestate.ListWorkersRequest{
		Filter:     &filter,
		PageSize:   pageSize,
		StartAfter: startAfter,
	})
	if err != nil {
		renderError(w, util.StatusWrap(err, "Failed to list worker state"))
		return
	}

	var nextStartAfter *buildqueuestate.ListWorkersRequest_StartAfter
	if l := response.Workers; len(l) > 0 {
		w := l[len(l)-1]
		nextStartAfter = &buildqueuestate.ListWorkersRequest_StartAfter{
			WorkerId: w.Id,
		}
	}

	if err := templates.ExecuteTemplate(w, "list_worker_state.html", struct {
		Filter         *buildqueuestate.ListWorkersRequest_Filter
		BrowserURL     *url.URL
		Now            time.Time
		PaginationInfo *buildqueuestate.PaginationInfo
		EndIndex       int
		StartAfter     *buildqueuestate.ListWorkersRequest_StartAfter
		Workers        []*buildqueuestate.WorkerState
	}{
		Filter:         &filter,
		BrowserURL:     s.browserURL,
		Now:            s.clock.Now(),
		PaginationInfo: response.PaginationInfo,
		EndIndex:       int(response.PaginationInfo.StartIndex) + len(response.Workers),
		StartAfter:     nextStartAfter,
		Workers:        response.Workers,
	}); err != nil {
		log.Print(err)
	}
}

func (s *buildQueueStateService) handleListDrains(w http.ResponseWriter, req *http.Request) {
	query := req.URL.Query()
	var sizeClassQueueName buildqueuestate.SizeClassQueueName
	if err := protojson.Unmarshal([]byte(query.Get("size_class_queue_name")), &sizeClassQueueName); err != nil {
		renderError(w, util.StatusWrapWithCode(err, codes.InvalidArgument, "Failed to extract size class queue name"))
		return
	}

	ctx := req.Context()
	response, err := s.buildQueue.ListDrains(ctx, &buildqueuestate.ListDrainsRequest{
		SizeClassQueueName: &sizeClassQueueName,
	})
	if err != nil {
		renderError(w, util.StatusWrap(err, "Failed to list drain state"))
		return
	}
	if err := templates.ExecuteTemplate(w, "list_drain_state.html", struct {
		SizeClassQueueName *buildqueuestate.SizeClassQueueName
		Now                time.Time
		Drains             []*buildqueuestate.DrainState
	}{
		SizeClassQueueName: &sizeClassQueueName,
		Now:                s.clock.Now(),
		Drains:             response.Drains,
	}); err != nil {
		log.Print(err)
	}
}

func handleModifyDrain(w http.ResponseWriter, req *http.Request, modifyFunc func(context.Context, *buildqueuestate.AddOrRemoveDrainRequest) (*emptypb.Empty, error)) {
	req.ParseForm()
	var sizeClassQueueName buildqueuestate.SizeClassQueueName
	if err := protojson.Unmarshal([]byte(req.FormValue("size_class_queue_name")), &sizeClassQueueName); err != nil {
		renderError(w, util.StatusWrapWithCode(err, codes.InvalidArgument, "Failed to extract size class queue name"))
		return
	}

	var workerIDPattern map[string]string
	if err := json.Unmarshal([]byte(req.FormValue("worker_id_pattern")), &workerIDPattern); err != nil {
		renderError(w, util.StatusWrapWithCode(err, codes.InvalidArgument, "Failed to extract worker ID pattern"))
		return
	}

	ctx := req.Context()
	if _, err := modifyFunc(ctx, &buildqueuestate.AddOrRemoveDrainRequest{
		SizeClassQueueName: &sizeClassQueueName,
		WorkerIdPattern:    workerIDPattern,
	}); err != nil {
		renderError(w, util.StatusWrap(err, "Failed to modify drains"))
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
