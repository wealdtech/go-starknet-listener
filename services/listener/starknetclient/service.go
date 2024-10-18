// Copyright © 2024 Weald Technology Limited.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package starknetclient

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	client "github.com/attestantio/go-starknet-client"
	jsonrpcclient "github.com/attestantio/go-starknet-client/jsonrpc"
	"github.com/cockroachdb/pebble"
	"github.com/rs/zerolog"
	zerologger "github.com/rs/zerolog/log"
	"github.com/wealdtech/go-starknet-listener/handlers"
)

// Service is a listener that listens to an Ethereum client.
type Service struct {
	log                 zerolog.Logger
	blockNumberProvider client.BlockNumberProvider
	blocksProvider      client.BlockProvider
	eventsProvider      client.EventsProvider
	blockTriggers       []*handlers.BlockTrigger
	txTriggers          []*handlers.TxTrigger
	eventTriggers       []*handlers.EventTrigger
	interval            time.Duration
	blockDelay          uint32
	blockSpecifier      string
	earliestBlock       int32
	metadataDB          *pebble.DB
	metadataDBMu        sync.Mutex
	metadataDBOpen      atomic.Bool
}

// New creates a new service.
func New(ctx context.Context, params ...Parameter) (*Service, error) {
	parameters, err := parseAndCheckParameters(params...)
	if err != nil {
		return nil, err
	}

	// Set logging.
	log := zerologger.With().Str("service", "listener").Str("impl", "starknetclient").Logger()
	if parameters.logLevel != log.GetLevel() {
		log = log.Level(parameters.logLevel)
	}

	if err := registerMetrics(ctx, parameters.monitor); err != nil {
		return nil, err
	}

	blockNumberPovider, blocksProvider, eventsProvider, err := setupProviders(ctx, parameters)
	if err != nil {
		return nil, err
	}

	metadataDB, err := pebble.Open(parameters.metadataDBPath, &pebble.Options{})
	if err != nil {
		return nil, errors.Join(errors.New("failed to start metadata database"), err)
	}

	s := &Service{
		log:                 log,
		metadataDB:          metadataDB,
		blocksProvider:      blocksProvider,
		eventsProvider:      eventsProvider,
		blockTriggers:       parameters.blockTriggers,
		txTriggers:          parameters.txTriggers,
		eventTriggers:       parameters.eventTriggers,
		blockDelay:          parameters.blockDelay,
		blockSpecifier:      parameters.blockSpecifier,
		earliestBlock:       parameters.earliestBlock,
		blockNumberProvider: blockNumberPovider,
		interval:            parameters.interval,
	}

	// Note that the metadata DB is open.
	s.metadataDBOpen.Store(true)

	// Close the database on context done.
	go func(ctx context.Context, metadataDB *pebble.DB) {
		<-ctx.Done()
		s.metadataDBMu.Lock()
		err := metadataDB.Close()
		s.metadataDBOpen.Store(false)
		s.metadataDBMu.Unlock()
		if err != nil {
			log.Warn().Err(err).Msg("Failed to close pebble")
		}
	}(ctx, metadataDB)

	// Kick off the listener.
	go s.listener(ctx)

	return s, nil
}

func setupProviders(ctx context.Context,
	parameters *parameters,
) (
	client.BlockNumberProvider,
	client.BlockProvider,
	client.EventsProvider,
	error,
) {
	jsonClient, err := jsonrpcclient.New(ctx,
		jsonrpcclient.WithLogLevel(parameters.clientLogLevel),
		jsonrpcclient.WithAddress(parameters.address),
		jsonrpcclient.WithTimeout(parameters.timeout),
	)
	if err != nil {
		return nil, nil, nil, errors.Join(errors.New("failed to connect to Starknet client"), err)
	}

	return jsonClient, jsonClient, jsonClient, nil
}
