package util

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/buildbarn/bb-storage/pkg/digest"
)

// GetPortalURL generates a URL pointing to an instance of bb-portal
// that can be visited to obtain more information about an object stored
// in the Content Addressable Storage (CAS) or Action Cache (AC).
func GetPortalURL(portalURL *url.URL, objectType string, digest digest.Digest) string {
	return portalURL.JoinPath(
		digest.GetInstanceName().String(),
		"blobs",
		strings.ToLower(digest.GetDigestFunction().GetEnumValue().String()),
		objectType,
		fmt.Sprintf("%s-%s", digest.GetHashString(), strconv.FormatInt(digest.GetSizeBytes(), 10)),
		"/",
	).String()
}
