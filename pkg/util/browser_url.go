package util

import (
	"fmt"
	"net/url"
	"path"
	"strconv"

	"github.com/buildbarn/bb-storage/pkg/digest"
)

// GetBrowserURL generates a URL that can be visited to obtain more
// information about an object stored in the Content Addressable Storage
// (CAS) or Action Cache (AC).
func GetBrowserURL(browserURL *url.URL, objectType string, digest digest.Digest) string {
	u, err := browserURL.Parse(
		path.Join(
			browserURL.EscapedPath(),
			objectType,
			digest.GetInstanceName().String(),
			digest.GetHashString(),
			strconv.FormatInt(digest.GetSizeBytes(), 10)) + "/")
	if err != nil {
		panic(fmt.Sprintf("Failed to create browser URL: %s", err))
	}
	return u.String()
}
