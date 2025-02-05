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

package handlers

import (
	"context"

	"github.com/attestantio/go-starknet-client/spec"
	"github.com/attestantio/go-starknet-client/types"
)

// TxTrigger is a trigger for a transaction.
type TxTrigger struct {
	Name          string
	From          *types.Address
	To            *types.Address
	EarliestBlock types.Number
	Handler       TxHandler
}

// TxHandlerFunc defines the handler function.
type TxHandlerFunc func(ctx context.Context, tx *spec.Transaction, trigger *TxTrigger)

// TxHandler defines the methods that need to be implemented to handle transactions.
type TxHandler interface {
	// HandleTx handles a transaction provided by the listener.
	// If this call returns an error then the listener will not send further transactions in the current poll,
	// and on the next poll it will start again with the this transaction.
	HandleTx(ctx context.Context, tx *spec.Transaction, trigger *TxTrigger) error
}
