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
	"os"
	"strings"
	"time"

	"cloud.google.com/go/bigtable"
	"go.opencensus.io/trace"
	"go.uber.org/zap"
	"google.golang.org/api/option"
)

const emulatorHostDefault = "BIGTABLE_EMULATOR_HOST"
const emulatorDefaultHostValue = "localhost:8086"

type ErrColumnNotPresent struct {
	familyColumn string
}

type ErrEmptyValue struct {
	familyColumn string
}

func (e *ErrColumnNotPresent) Error() string {
	return fmt.Sprintf("column '%s' not present", e.familyColumn)
}

func NewErrColumnNotPresent(familyColumn string) *ErrColumnNotPresent {
	return &ErrColumnNotPresent{
		familyColumn: familyColumn,
	}
}

func IsErrColumnNotPresent(err error) bool {
	switch err.(type) {
	case *ErrColumnNotPresent:
		return true
	default:
		return false
	}
}

func NewErrEmptyValue(familyColumn string) *ErrEmptyValue {
	return &ErrEmptyValue{
		familyColumn: familyColumn,
	}
}

func IsErrEmptyValue(err error) bool {
	switch err.(type) {
	case *ErrEmptyValue:
		return true
	default:
		return false
	}
}

func (e *ErrEmptyValue) Error() string {
	return fmt.Sprintf("value '%s' present but empty", e.familyColumn)
}

type Bigtable struct {
	client                 *bigtable.Client
	tablePrefix            string
	tables                 []*BaseTable
	maxDurationBeforeFlush time.Duration
	maxBlocksBeforeFlush   uint64
	lastFlushTime          time.Time
	blocksSinceFlush       uint64
}

func NewWithClient(tablePrefix string, tables []*BaseTable, client *bigtable.Client) *Bigtable {
	bt := &Bigtable{
		client:      client,
		tablePrefix: tablePrefix,
		tables:      tables,
	}

	return bt
}

func New(tablePrefix, project, instance string, tables []*BaseTable, createTables bool, opts ...option.ClientOption) (*Bigtable, error) {
	ctx := context.Background()

	optionalTestEnv(project, instance)

	client, err := bigtable.NewClient(ctx, project, instance, opts...)
	if err != nil {
		return nil, err
	}

	return NewWithClient(tablePrefix, tables, client), nil
}

func (b *Bigtable) CreateTables(ctx context.Context, project, instance string, opts ...option.ClientOption) {
	zlog.Info("creating bigtable tables")
	adminClient, err := bigtable.NewAdminClient(ctx, project, instance, opts...)
	if err != nil {
		zlog.Error("unable to create bigtable admin client, unable to create tables", zap.Error(err))
	} else {
		for _, table := range b.tables {
			table.EnsureTableAndFamiliesExist(adminClient)
		}
	}
}

func (b *Bigtable) Flush(ctx context.Context) error {
	return b.FlushAllMutations(ctx)
}

func (b *Bigtable) Close() error {
	return b.client.Close()
}

func (b *Bigtable) IncrementPutBlockCounter() {
	b.blocksSinceFlush++
}

func (b *Bigtable) ShouldFlushMutations() bool {
	return b.blocksSinceFlush >= b.maxBlocksBeforeFlush || time.Since(b.lastFlushTime) > b.maxDurationBeforeFlush
}

func (b *Bigtable) FlushAllMutations(ctx context.Context) error {
	for _, table := range b.tables {
		if err := table.FlushMutations(ctx); err != nil {
			return fmt.Errorf("error flushing %s: %w", table.Name, err)
		}
	}

	return nil
}

func (b *Bigtable) StartSpan(ctx context.Context, protocol string, name string, attributes ...trace.Attribute) (context.Context, *trace.Span) {
	childCtx, span := trace.StartSpan(ctx, fmt.Sprintf("kvdb/%s/%s", protocol, name))
	span.AddAttributes(append(attributes, trace.StringAttribute("table_prefix", b.tablePrefix))...)

	return childCtx, span
}

func optionalTestEnv(project, instance string) {
	if isTestEnv(project, instance) && (os.Getenv(emulatorHostDefault) == "") {
		os.Setenv(emulatorHostDefault, emulatorDefaultHostValue)
	}
}

func isTestEnv(project, instance string) bool {
	return (strings.HasPrefix(project, "dev") || strings.HasPrefix(instance, "dev"))
}
