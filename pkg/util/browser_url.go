package util

import (
	"fmt"
	"net/url"
	"strconv"

	"github.com/buildbarn/bb-storage/pkg/digest"
)

// GetBrowserURL generates a URL that can be visited to obtain more
// information about an object stored in the Content Addressable Storage
// (CAS) or Action Cache (AC).
func GetBrowserURL(browserURL *url.URL, objectType string, digest digest.Digest) string {
	return browserURL.JoinPath(
		digest.GetInstanceName().String(),
		"blobs",
		objectType,
		fmt.Sprintf("%s-%s", digest.GetHashString(), strconv.FormatInt(digest.GetSizeBytes(), 10)),
		"/",
	).String()
}
