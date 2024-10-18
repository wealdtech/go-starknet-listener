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
	"fmt"
	"time"

	"github.com/attestantio/go-starknet-client/api"
	"github.com/attestantio/go-starknet-client/spec"
	"github.com/attestantio/go-starknet-client/types"
	"github.com/cespare/xxhash/v2"
	"github.com/rs/zerolog/log"
	"github.com/wealdtech/go-starknet-listener/handlers"
)

// Maximum number of events to fetch.
const maxEvents = uint32(1024)

func (s *Service) listener(ctx context.Context,
) {
	// Start with a poll.
	s.poll(ctx)

	// Now loop until context is cancelled.
	for {
		select {
		case <-time.After(s.interval):
			s.poll(ctx)
		case <-ctx.Done():
			s.log.Debug().Msg("Context done")
			return
		}
	}
}

//nolint:revive
func (s *Service) selectHighestBlock(ctx context.Context) (uint32, error) {
	var to uint32
	// Select the highest block with which to work, based on the specifier or the block delay.
	if s.blockSpecifier != "" {
		return 0, errors.New("starknet does not currently provide block specifiers")
		// block, err := s.blocksProvider.Block(ctx, &api.BlockOpts{Block: s.blockSpecifier})
		// if err != nil {
		// 	return 0, errors.Join(errors.New("failed to obtain block"), err)
		// }
		// to = block.Number()
		// s.log.Trace().Str("specifier", s.blockSpecifier).Uint32("height", to).Msg("Obtained chain height with specifier")
	} else {
		blockNumberResponse, err := s.blockNumberProvider.BlockNumber(ctx, &api.BlockNumberOpts{})
		if err != nil {
			return 0, errors.Join(errors.New("failed to get chain height for event poll"), err)
		}
		chainHeight := blockNumberResponse.Data
		to = chainHeight - s.blockDelay
		s.log.Trace().Uint32("block_delay", s.blockDelay).Uint32("height", to).Msg("Obtained chain height with delay")
	}

	s.log.Trace().Uint32("height", to).Msg("Selected highest block")

	return to, nil
}

func (s *Service) poll(ctx context.Context) {
	to, err := s.selectHighestBlock(ctx)
	if err != nil && ctx.Err() == nil {
		s.log.Error().Err(err).Msg("Failed to select highest block")
		monitorFailure()

		return
	}

	s.pollTo(ctx, to)
}

func (s *Service) pollTo(ctx context.Context, to uint32) {
	s.pollBlocksTo(ctx, to)
	// s.pollTxsTo(ctx, to)
	s.pollEventsTo(ctx, to)
	monitorLatestBlock(to)
}

func (s *Service) pollBlocksTo(ctx context.Context, to uint32) {
	if len(s.blockTriggers) > 0 {
		s.log.Trace().Msg("Polling blocks")
		err := s.pollBlocks(ctx, to)
		if err != nil && ctx.Err() == nil {
			s.log.Error().Err(err).Msg("Block poll failed")
			monitorFailure()
		}
	}
}

// func (s *Service) pollTxsTo(ctx context.Context, to uint32) {
// 	if len(s.txTriggers) > 0 {
// 		s.log.Trace().Msg("Polling blocks for transactions")
// 		err := s.pollTxs(ctx, to)
// 		if err != nil && ctx.Err() == nil {
// 			s.log.Error().Err(err).Msg("Transaction poll failed")
// 			monitorFailure()
// 		}
// 	}
// }

func (s *Service) pollEventsTo(ctx context.Context, to uint32) {
	if len(s.eventTriggers) > 0 {
		s.log.Trace().Msg("Polling events")
		err := s.pollEvents(ctx, to)
		if err != nil && ctx.Err() == nil {
			s.log.Error().Err(err).Msg("Event poll failed")
			monitorFailure()
		}
	}
}

func (s *Service) pollBlocks(ctx context.Context,
	to uint32,
) error {
	md, err := s.getBlocksMetadata(ctx)
	if err != nil {
		return errors.Join(errors.New("failed to get metadata for block poll"), err)
	}

	from := s.calculateBlocksFrom(ctx, md)
	s.log.Trace().Uint32("from", from).Uint32("to", to).Msg("Polling blocks in range")
	if from > to {
		return nil
	}

	failed := make(map[string]bool)
	for height := from; height <= to; height++ {
		s.log.Trace().Uint32("block", height).Msg("Handling block")
		blockResponse, err := s.blocksProvider.Block(ctx, &api.BlockOpts{
			Block: fmt.Sprintf("%d", height),
		})
		if err != nil {
			return errors.Join(errors.New("failed to obtain block"), err)
		}

		for _, trigger := range s.blockTriggers {
			if failed[trigger.Name] {
				// The trigger already reported a failure in this run, so don't run for future blocks.
				continue
			}
			if md.LatestBlocks[trigger.Name] >= int32(height) {
				// The trigger has already successfully processed this block.
				continue
			}
			if err := trigger.Handler.HandleBlock(ctx, blockResponse.Data, trigger); err != nil {
				s.log.Debug().Str("trigger", trigger.Name).Uint32("block", height).Err(err).Msg("Trigger failed to handle block")
				// The trigger has reported a failure.  We stop here for this trigger and don't update its metadata.
				failed[trigger.Name] = true

				continue
			}
			md.LatestBlocks[trigger.Name] = int32(height)
		}

		if err := s.setBlocksMetadata(ctx, md); err != nil {
			return errors.Join(errors.New("failed to set metadata after block poll"), err)
		}
	}

	return nil
}

const maxUint32 = uint32(0xffffffff)

// calculateBlocksFrom calculates the earliest block which we need to fetch.
func (s *Service) calculateBlocksFrom(_ context.Context, md *blocksMetadata) uint32 {
	var from uint32

	switch {
	case s.earliestBlock > -1:
		// There is a hard-coded earliest block passed to us in configuration, so we must start there.
		// We have to reset the metadata, otherwise blocks won't be reprocessed.
		from = uint32(s.earliestBlock)
		for name := range md.LatestBlocks {
			md.LatestBlocks[name] = s.earliestBlock - 1
		}
		s.earliestBlock = -1
	case len(md.LatestBlocks) > 0:
		// Work out the earliest block from our existing metadata.
		from = maxUint32
		for _, latest := range md.LatestBlocks {
			if from > uint32(latest+1) {
				from = uint32(latest + 1)
			}
		}
	default:
		// Means that there is no metadata or hard-coded block, so start from the beginning.
		from = 0
	}

	return from
}

// func (s *Service) pollTxs(ctx context.Context,
// 	to uint32,
// ) error {
// 	md, err := s.getTransactionsMetadata(ctx)
// 	if err != nil {
// 		return errors.Join(errors.New("failed to get metadata for transaction poll"), err)
// 	}
//
// 	from := uint32(md.LatestBlock + 1)
// 	if s.earliestBlock != -1 {
// 		from = uint32(s.earliestBlock)
// 		s.earliestBlock = -1
// 	}
//
// 	if from > to {
// 		s.log.Trace().Uint32("from", from).Uint32("to", to).Msg("Not fetching blocks for transactions")
// 		return nil
// 	}
//
// 	for height := from; height <= to; height++ {
// 		if err := s.pollBlockTxs(ctx, height); err != nil {
// 			return err
// 		}
//
// 		md.LatestBlock = int32(height)
// 		if err := s.setTransactionsMetadata(ctx, md); err != nil {
// 			return errors.Join(errors.New("failed to set metadata after trasaction poll"), err)
// 		}
// 	}
//
// 	return nil
// }
//
// func (s *Service) pollBlockTxs(ctx context.Context, height uint32) error {
// 	block, err := s.blocksProvider.Block(ctx, util.MarshalUint32(height))
// 	if err != nil {
// 		return errors.Join(errors.New("failed to obtain block for transactions"), err)
// 	}
//
// 	log := s.log.With().Uint32("block_height", block.Number()).Logger()
// 	for _, trigger := range s.txTriggers {
// 		log := log.With().Str("trigger", trigger.Name).Logger()
// 		if block.Number() < trigger.EarliestBlock {
// 			log.Trace().Msg("Block too early; ignoring")
// 			continue
// 		}
// 		for i, tx := range block.Transactions() {
// 			if trigger.From != nil {
// 				txFrom := tx.From()
// 				if !bytes.Equal(trigger.From[:], txFrom[:]) {
// 					log.Trace().Int("index", i).Msg("From does not match; ignoring")
// 					continue
// 				}
// 			}
// 			if trigger.To != nil {
// 				txTo := tx.To()
// 				if !bytes.Equal(trigger.To[:], txTo[:]) {
// 					log.Trace().Int("index", i).Msg("To does not match; ignoring")
// 					continue
// 				}
// 			}
// 			trigger.Handler.HandleTx(ctx, tx, trigger)
// 		}
// 	}
//
// 	return nil
// }

func (s *Service) pollEvents(ctx context.Context,
	toBlock uint32,
) error {
	md, err := s.getEventsMetadata(ctx)
	if err != nil {
		return errors.Join(errors.New("failed to get metadata for event poll"), err)
	}

	// Need to run each trigger separately.
	for _, trigger := range s.eventTriggers {
		// Obtain the last block and transaction we examined for this trigger, or use the earliest block as defined in the trigger.
		fromBlock := trigger.EarliestBlock
		fromEventHash := uint64(0)
		if entry, exists := md.Entries[trigger.Name]; exists {
			if entry.LatestBlock >= fromBlock {
				fromBlock = entry.LatestBlock
				fromEventHash = entry.LatestEventHash
			}
		} else {
			md.Entries[trigger.Name] = &eventsEntryMetadata{
				LatestBlock:     fromBlock,
				LatestEventHash: fromEventHash,
			}
		}
		if fromBlock > toBlock {
			s.log.Trace().
				Str("trigger", trigger.Name).
				Uint32("from_block", fromBlock).
				Uint64("from_event_index", fromEventHash).
				Uint32("to_block", toBlock).
				Msg("Not fetching events")

			return nil
		}

		latestBlock, latestEventHash, err := s.pollEventsForTrigger(ctx, trigger, fromBlock, fromEventHash, toBlock)
		if err != nil {
			s.log.Debug().
				Str("trigger", trigger.Name).
				Uint32("latest_block", latestBlock).
				Uint64("latest_event_hash", latestEventHash).
				Err(err).
				Msg("Poll errored")
		}
		md.Entries[trigger.Name].LatestBlock = latestBlock
		md.Entries[trigger.Name].LatestEventHash = latestEventHash

		if err := s.setEventsMetadata(ctx, md); err != nil {
			return errors.Join(errors.New("failed to set metadata after event poll"), err)
		}
	}

	return nil
}

func (s *Service) pollEventsForTrigger(ctx context.Context,
	trigger *handlers.EventTrigger,
	fromBlock uint32,
	fromEventHash uint64,
	toBlock uint32,
) (
	uint32,
	uint64,
	error,
) {
	log := s.log.With().Str("trigger", trigger.Name).Logger()

	events, err := s.eventsForTrigger(ctx, trigger, fromBlock, fromEventHash, toBlock)
	if err != nil {
		return fromBlock, fromEventHash, err
	}

	searching := fromEventHash != 0
	latestBlock := fromBlock
	latestEventHash := fromEventHash
	for _, event := range events {
		eventHash := xxhash.Sum64([]byte(event.String()))
		log := log.With().
			Uint32("block_number", *event.BlockNumber).
			Stringer("tx", event.TransactionHash).
			Uint64("event_hash", eventHash).
			Logger()

		if searching {
			// We have found the ltest event we processed, so stop searching.
			if *event.BlockNumber == latestBlock && eventHash == latestEventHash {
				searching = false
			}

			continue
		}

		if err := trigger.Handler.HandleEvent(ctx, event, trigger); err != nil {
			log.Trace().Err(err).Msg("Handler errored")

			return latestBlock, latestEventHash, errors.Join(errors.New("handler errored"), err)
		}
		log.Trace().Msg("Handler succeeded")

		latestBlock = *event.BlockNumber
		latestEventHash = eventHash
	}

	// We have processed all of the events for the blocks.
	return toBlock + 1, 0, nil
}

func (s *Service) eventsForTrigger(ctx context.Context,
	trigger *handlers.EventTrigger,
	fromBlock uint32,
	fromEventHash uint64,
	toBlock uint32,
) (
	[]*spec.TransactionEvent,
	error,
) {
	source, err := s.resolveSourceFromTrigger(ctx, trigger)
	if err != nil {
		return nil, err
	}

	s.log.Trace().
		Str("trigger", trigger.Name).
		Uint32("from_block", fromBlock).
		Uint64("from_event_hash", fromEventHash).
		Uint32("to", toBlock).
		Msg("Fetching events")

	eventsOpts := &api.EventsOpts{
		FromBlock: fmt.Sprintf("%d", fromBlock),
		ToBlock:   fmt.Sprintf("%d", toBlock),
		Limit:     maxEvents,
	}
	if source != nil {
		eventsOpts.Address = source
	}
	if len(trigger.Topics) > 0 {
		eventsOpts.Keys = trigger.Topics
	}

	eventsResponse, err := s.eventsProvider.Events(ctx, eventsOpts)
	if err != nil {
		return nil, errors.Join(errors.New("failed to obtain events"), err)
	}
	events := eventsResponse.Data
	s.log.Info().
		Str("trigger", trigger.Name).
		Int("events", len(events)).
		Msg("Obtained events")

	return events, nil
}

func (s *Service) resolveSourceFromTrigger(ctx context.Context,
	trigger *handlers.EventTrigger,
) (
	*types.Address,
	error,
) {
	// Resolve the source, if present.
	var source *types.Address
	var err error
	switch {
	case trigger.SourceResolver != nil:
		source, err = trigger.SourceResolver.Resolve(ctx)
		if err != nil {
			return nil, errors.Join(errors.New("failed to resolve source"), err)
		}
	case trigger.Source != nil:
		source = trigger.Source
	}
	if source != nil {
		log.Trace().Stringer("source", source).Msg("Source to be used for events")
	}

	return source, nil
}
