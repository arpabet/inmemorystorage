/**
  Copyright (c) 2022 Arpabet, LLC. All rights reserved.
*/

package inmemorystorage

import (
	"github.com/patrickmn/go-cache"
	"time"
)

func OpenDatabase(options ...Option) *cache.Cache {

	conf := &Config{
		DefaultExpiration: cache.NoExpiration,
		CleanupInterval:  time.Hour,
	}

	for _, opt := range options {
		opt.apply(conf)
	}

	return cache.New(conf.DefaultExpiration, conf.CleanupInterval)
}




