package invocation

import (
	"fmt"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/buildqueuestate"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

// Key for identifying client invocations. InMemoryBuildQueue uses this
// type to group operations within a given size class queue. This
// grouping is used to introduce fairness between builds.
//
// For most setups, it is sufficient to set the Key to the tool
// invocation ID that's part of the RequestMetadata header. For more
// advanced setups, it may be recommended to include information such as
// the username.
type Key string

// NewKey creates a new key based on a freeform Protobuf message.
func NewKey(id *anypb.Any) (Key, error) {
	data, err := protojson.Marshal(id)
	if err != nil {
		return "", util.StatusWrap(err, "Failed to marshal invocation ID")
	}
	return Key(data), nil
}

// GetID reobtains the Protobuf message that was used to construct the
// key.
func (k Key) GetID() *anypb.Any {
	var id anypb.Any
	if err := protojson.Unmarshal([]byte(k), &id); err != nil {
		panic(fmt.Sprintf("Failed to unmarshal previously marshalled invocation ID: %s", err))
	}
	return &id
}

// BackgroundLearningKeys is a predefined list of Keys that is used for
// all operations that are created to perform background learning (see
// initialsizeclass.FeedbackAnalyzer).
var BackgroundLearningKeys = []Key{
	util.Must(NewKey(util.Must(anypb.New(&buildqueuestate.BackgroundLearning{})))),
}
