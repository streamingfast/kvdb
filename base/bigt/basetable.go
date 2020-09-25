// Copyright 2019 dfuse Platform Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package basebigt

import (
	"context"
	"fmt"
	"strings"

	"cloud.google.com/go/bigtable"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type BaseTable struct {
	// We "inherit" from `Table` so that `ReadRows` and `ReadRow` is available directly on the instance.
	*bigtable.Table

	Name     string
	Families []string

	pendingSets []*SetEntry
}

type SetEntry struct {
	Key    string
	Family string
	Column string
	Value  []byte
}

func NewBaseTable(name string, families []string, client *bigtable.Client) *BaseTable {
	return &BaseTable{
		Table:    client.Open(name),
		Families: families,
		Name:     name,
	}
}

func (b *BaseTable) PendingSets() []*SetEntry {
	return b.pendingSets
}

func (b *BaseTable) SetKey(key string, familyColumn string, value []byte) {
	chunks := strings.SplitN(familyColumn, ":", 2)
	if len(chunks) != 2 {
		panic("that one's on you my friend, familyColumn is always family:column")
	}

	b.pendingSets = append(b.pendingSets, &SetEntry{
		Key:    key,
		Family: chunks[0],
		Column: chunks[1],
		Value:  value,
	})
}

func (b *BaseTable) FlushMutations(ctx context.Context) error {
	if len(b.pendingSets) == 0 {
		return nil
	}

	zlog.Debug("number of entries in table before flushing", zap.String("table_name", b.Name), zap.Int("length_pending_sets", len(b.pendingSets)))
	var keys []string
	var mutations []*bigtable.Mutation
	var pendingSize uint64
	for _, s := range b.pendingSets {
		keys = append(keys, s.Key)
		mut := bigtable.NewMutation()
		mut.Set(s.Family, s.Column, bigtable.Now(), s.Value)
		mutations = append(mutations, mut)
		pendingSize += uint64(len(s.Key) + len(s.Family) + len(s.Value) + 20)
		if len(mutations) > 85000 || pendingSize > 85000000 {
			zlog.Debug("flushing chunk in the middle of a FlushMutation operation", zap.String("table_name", b.Name), zap.Int("length_mutations", len(mutations)), zap.Uint64("size_mutations", pendingSize))
			err := b.doFlushMutations(ctx, keys, mutations)
			if err != nil {
				return err
			}
			pendingSize = 0
			mutations = nil
			keys = nil
		}
	}

	err := b.doFlushMutations(ctx, keys, mutations)
	if err != nil {
		return err
	}

	b.pendingSets = nil

	return nil
}

func (b *BaseTable) doFlushMutations(ctx context.Context, keys []string, mutations []*bigtable.Mutation) error {
	errs, err := b.ApplyBulk(ctx, keys, mutations)
	if err != nil {
		return err
	}
	if len(errs) != 0 {
		return fmt.Errorf("apply bulk error for table %s: %w", b.Name, multierr.Combine(errs...))
	}

	return nil
}

func (b *BaseTable) EnsureTableAndFamiliesExist(admin *bigtable.AdminClient) {
	ctx := context.Background()

	zlog.Info("creating table", zap.String("name", b.Name))
	if err := admin.CreateTable(ctx, b.Name); err != nil && !isAlreadyExistsError(err) {
		zlog.Error("failed creating table", zap.String("name", b.Name), zap.Error(err))
	}

	for _, family := range b.Families {
		if err := admin.CreateColumnFamily(ctx, b.Name, family); err != nil && !isAlreadyExistsError(err) {
			zlog.Error("failed creating family", zap.String("table_name", b.Name), zap.String("family", family), zap.Error(err))
		}

		if err := admin.SetGCPolicy(ctx, b.Name, family, bigtable.MaxVersionsPolicy(1)); err != nil {
			zlog.Error("failed applying gc policy", zap.String("table_name", b.Name), zap.String("family", family), zap.Error(err))
		}
	}
}

func isAlreadyExistsError(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return false
	}

	return st.Code() == codes.AlreadyExists
}
