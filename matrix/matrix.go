package matrix

import (
	"sync"

	"github.com/rubyist/circuitbreaker"
	"github.com/turt2live/matrix-media-repo/common"
	"github.com/turt2live/matrix-media-repo/common/config"
)

var breakers = &sync.Map{}

func getBreakerAndConfig(serverName string) (*config.DomainRepoConfig, *circuit.Breaker) {
	hs := config.GetDomain(serverName)

	var cb *circuit.Breaker
	cbRaw, hasCb := breakers.Load(hs.Name)
	if !hasCb {
		backoffAt := int64(hs.BackoffAt)
		if backoffAt <= 0 {
			backoffAt = 10 // default to 10 for those who don't have this set
		}
		cb = circuit.NewConsecutiveBreaker(backoffAt)
		breakers.Store(hs.Name, cb)
	} else {
		cb = cbRaw.(*circuit.Breaker)
	}

	return hs, cb
}

func filterError(err error) (error, error) {
	if err == nil {
		return nil, nil
	}

	// Unknown token errors should be filtered out explicitly to ensure we don't break on bad requests
	if httpErr, ok := err.(*errorResponse); ok {
		if httpErr.ErrorCode == common.ErrCodeUnknownToken {
			// We send back our own version of 'unknown token' to ensure we can filter it out elsewhere
			return nil, ErrInvalidToken
		}
	}

	return err, err
}
