package clock

import (
	"context"
	"net/http"
	"time"

	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/program"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func callHTTPSuspender(ctx context.Context, httpClient *http.Client, url string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return util.StatusWrap(err, "Cannot create request")
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return util.StatusWrap(err, "Cannot execute request")
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return status.Errorf(codes.Internal, "Server returned unexpected status %#v", resp.Status)
	}
	return nil
}

func callHTTPSuspenderRetried(ctx context.Context, httpClient *http.Client, url string, errorLogger util.ErrorLogger, clock clock.Clock) bool {
	for {
		err := callHTTPSuspender(ctx, httpClient, url)
		if err == nil {
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		errorLogger.Log(util.StatusWrapf(err, "Failed to fetch %#v", url))

		timer, timerChan := clock.NewTimer(time.Second)
		select {
		case <-timerChan:
		case <-ctx.Done():
			timer.Stop()
			return false
		}
	}
}

// LaunchHTTPSuspender spawns a goroutine that repeatedly calls into a
// service via HTTP to determine whether to suspend or resume a
// Suspendable.
//
// This can be used to let workers compensate the execution timeouts of
// workers for delays that are outside the worker's control, such as the
// overhead imposed by network file systems.
func LaunchHTTPSuspender(group program.Group, suspendable Suspendable, httpClient *http.Client, suspendURL, resumeURL string, errorLogger util.ErrorLogger, clock clock.Clock) {
	group.Go(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
		for {
			if !callHTTPSuspenderRetried(ctx, httpClient, suspendURL, errorLogger, clock) {
				return nil
			}
			suspendable.Suspend()

			if !callHTTPSuspenderRetried(ctx, httpClient, resumeURL, errorLogger, clock) {
				suspendable.Resume()
				return nil
			}
			suspendable.Resume()
		}
	})
}
