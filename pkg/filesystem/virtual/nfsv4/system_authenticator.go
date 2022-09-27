package nfsv4

import (
	"bytes"
	"context"
	"crypto/sha256"
	"log"
	"sync"

	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/eviction"
	"github.com/buildbarn/go-xdr/pkg/protocols/rpcv2"
	"github.com/buildbarn/go-xdr/pkg/rpcserver"
	"github.com/jmespath/go-jmespath"
)

// SystemAuthenticatorCacheKey is the key type that's used by
// the system authenticato's eviction set.
type SystemAuthenticatorCacheKey [sha256.Size]byte

type systemAuthenticator struct {
	metadataExtractor *jmespath.JMESPath

	lock             sync.Mutex
	cache            map[SystemAuthenticatorCacheKey]*auth.AuthenticationMetadata
	maximumCacheSize int
	evictionSet      eviction.Set[SystemAuthenticatorCacheKey]
}

// NewSystemAuthenticator is an RPCv2 Authenticator that requires that
// requests provide credentials of flavor AUTH_SYS, as described in RFC
// 5531, appendix A. The body of the credentials are converted to an
// AuthenticationMetadata object using a JMESPath expression. The
// resulting metadata is attached to the Context.
func NewSystemAuthenticator(metadataExtractor *jmespath.JMESPath, maximumCacheSize int, evictionSet eviction.Set[SystemAuthenticatorCacheKey]) rpcserver.Authenticator {
	return &systemAuthenticator{
		metadataExtractor: metadataExtractor,

		maximumCacheSize: maximumCacheSize,
		cache:            map[SystemAuthenticatorCacheKey]*auth.AuthenticationMetadata{},
		evictionSet:      evictionSet,
	}
}

func (a *systemAuthenticator) Authenticate(ctx context.Context, credentials, verifier *rpcv2.OpaqueAuth) (context.Context, rpcv2.OpaqueAuth, rpcv2.AuthStat) {
	switch credentials.Flavor {
	case rpcv2.AUTH_SYS:
		key := sha256.Sum256(credentials.Body)

		a.lock.Lock()
		defer a.lock.Unlock()

		authenticationMetadata, ok := a.cache[key]
		if ok {
			// Client either ignores AUTH_SHORT, or the
			// request got retransmitted. Return the cached
			// credentials.
			a.evictionSet.Touch(key)
		} else {
			// Parse system authentication data.
			var credentialsBody rpcv2.AuthsysParms
			b := bytes.NewBuffer(credentials.Body)
			if _, err := credentialsBody.ReadFrom(b); err != nil || b.Len() != 0 {
				return nil, rpcv2.OpaqueAuth{}, rpcv2.AUTH_BADCRED
			}

			// Convert to authentication metadata.
			gids := make([]any, 0, len(credentialsBody.Gids))
			for _, gid := range credentialsBody.Gids {
				gids = append(gids, gid)
			}
			raw, err := a.metadataExtractor.Search(map[string]any{
				"stamp":       credentialsBody.Stamp,
				"machinename": credentialsBody.Machinename,
				"uid":         credentialsBody.Uid,
				"gid":         credentialsBody.Gid,
				"gids":        gids,
			})
			if err != nil {
				return nil, rpcv2.OpaqueAuth{}, rpcv2.AUTH_BADCRED
			}
			authenticationMetadata, err = auth.NewAuthenticationMetadataFromRaw(raw)
			if err != nil {
				log.Print("Failed to create authentication metadata: ", err)
				return nil, rpcv2.OpaqueAuth{}, rpcv2.AUTH_BADCRED
			}

			// Insert the authentication metadata into the cache.
			for len(a.cache) > 0 && len(a.cache) > a.maximumCacheSize {
				delete(a.cache, a.evictionSet.Peek())
				a.evictionSet.Remove()
			}
			a.cache[key] = authenticationMetadata
			a.evictionSet.Insert(key)
		}
		return auth.NewContextWithAuthenticationMetadata(ctx, authenticationMetadata),
			rpcv2.OpaqueAuth{
				Flavor: rpcv2.AUTH_SHORT,
				Body:   key[:],
			},
			rpcv2.AUTH_OK
	case rpcv2.AUTH_SHORT:
		// Client is referring back to previously used
		// credentials. Look up the credentials from the cache.
		if len(credentials.Body) != sha256.Size {
			return nil, rpcv2.OpaqueAuth{}, rpcv2.AUTH_BADCRED
		}
		var key SystemAuthenticatorCacheKey
		copy(key[:], credentials.Body)

		a.lock.Lock()
		defer a.lock.Unlock()

		if authenticationMetadata, ok := a.cache[key]; ok {
			a.evictionSet.Touch(key)
			return auth.NewContextWithAuthenticationMetadata(ctx, authenticationMetadata),
				rpcv2.OpaqueAuth{Flavor: rpcv2.AUTH_NONE},
				rpcv2.AUTH_OK
		}

		// Client needs to provide the original credentials
		// again, as they are no longer present in the cache.
		return nil, rpcv2.OpaqueAuth{}, rpcv2.AUTH_REJECTEDCRED
	default:
		return nil, rpcv2.OpaqueAuth{}, rpcv2.AUTH_BADCRED
	}
}
