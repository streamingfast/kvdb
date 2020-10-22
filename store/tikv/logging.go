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

package tikv

import (
	"fmt"
	"os"

	logrusToZap "github.com/Sytten/logrus-zap-hook"
	"github.com/dfuse-io/logging"
	"github.com/sirupsen/logrus"
	"github.com/tikv/client-go/config"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var traceEnabled = os.Getenv("TRACE") == "true"
var zlog *zap.Logger

func init() {
	logging.Register("github.com/dfuse-io/kvdb/store/tikv", &zlog)

	hook, err := logrusToZap.NewZapHook(zlog)
	if err != nil {
		panic(fmt.Errorf("at time of writing, the library was not emitting any error even if in the interface, it seems it does now: %w", err))
	}

	logrus.StandardLogger().AddHook(hook)
}

type tikvConfigRaw config.Raw

func (c tikvConfigRaw) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	v := config.Raw(c)

	enc.AddInt("max_scan_limit", v.MaxScanLimit)
	enc.AddInt("max_batch_put_size", v.MaxBatchPutSize)
	enc.AddInt("batch_pair_count", v.BatchPairCount)
	return nil
}
