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
	"io/ioutil"
	"runtime"

	"github.com/dfuse-io/logging"
	"github.com/sirupsen/logrus"
	"github.com/tikv/client-go/config"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var traceEnabled = logging.IsTraceEnabled("kvdb", "github.com/dfuse-io/kvdb/tikv")
var zlog *zap.Logger

func init() {
	hook := &logrusHook{}

	logging.Register("github.com/dfuse-io/kvdb/store/tikv", &zlog, logging.RegisterOnUpdate(func(newLogger *zap.Logger) {
		hook.logger = newLogger.Named("tikv-client")
		reconfigureLogrusLevel(hook.logger)
	}))

	// The code here is used to re-configured standard logger on `logrus` library which is used
	// by tikv-client to log stuff.
	//
	// Ideally, we would like to configure it "per tikv store" instance, but this is not possible
	// since the library uses only the standard global logger. Created an issue to track so if it
	// change at some point, we will revisit this.
	//
	// See https://github.com/tikv/client-go/issues/59
	logrus.StandardLogger().AddHook(hook)
	logrus.StandardLogger().SetOutput(ioutil.Discard)
	logrus.StandardLogger().SetReportCaller(true)
}

type tikvConfigRaw config.Raw

func (c tikvConfigRaw) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	v := config.Raw(c)

	enc.AddInt("max_scan_limit", v.MaxScanLimit)
	enc.AddInt("max_batch_put_size", v.MaxBatchPutSize)
	enc.AddInt("batch_pair_count", v.BatchPairCount)
	return nil
}

type logrusHook struct {
	logger *zap.Logger
}

func (h *logrusHook) Fire(entry *logrus.Entry) error {
	if h.logger == nil {
		return nil
	}

	fields := make([]zap.Field, len(entry.Data))

	i := 0
	for key, value := range entry.Data {
		if key == logrus.ErrorKey {
			fields[i] = zap.Error(value.(error))
		} else {
			fields[i] = zap.Any(key, value)
		}
		i++
	}

	switch entry.Level {
	case logrus.PanicLevel:
		h.write(zapcore.PanicLevel, entry.Message, fields, entry.Caller)
	case logrus.FatalLevel:
		h.write(zapcore.FatalLevel, entry.Message, fields, entry.Caller)
	case logrus.ErrorLevel:
		h.write(zapcore.ErrorLevel, entry.Message, fields, entry.Caller)
	case logrus.WarnLevel:
		h.write(zapcore.WarnLevel, entry.Message, fields, entry.Caller)
	case logrus.InfoLevel:
		h.write(zapcore.InfoLevel, entry.Message, fields, entry.Caller)
	case logrus.DebugLevel, logrus.TraceLevel:
		h.write(zapcore.DebugLevel, entry.Message, fields, entry.Caller)
	}

	return nil
}

func (h *logrusHook) write(lvl zapcore.Level, msg string, fields []zap.Field, caller *runtime.Frame) {
	if ce := h.logger.Check(lvl, msg); ce != nil {
		if caller != nil {
			ce.Caller = zapcore.NewEntryCaller(caller.PC, caller.File, caller.Line, caller.PC != 0)
		}
		ce.Write(fields...)
	}
}

func (h *logrusHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

func reconfigureLogrusLevel(logger *zap.Logger) {
	level := inferZapLoggerLevel(logger)

	switch level {
	case zap.DebugLevel:
		logrus.SetLevel(logrus.DebugLevel)
	case zap.InfoLevel:
		logrus.SetLevel(logrus.InfoLevel)
	case zap.WarnLevel:
		logrus.SetLevel(logrus.WarnLevel)
	case zap.ErrorLevel:
		logrus.SetLevel(logrus.ErrorLevel)
	}
}

func inferZapLoggerLevel(logger *zap.Logger) zapcore.Level {
	core := logger.Core()
	if core.Enabled(zap.DebugLevel) {
		return zap.DebugLevel
	}

	if core.Enabled(zap.InfoLevel) {
		return zap.InfoLevel
	}

	if core.Enabled(zap.WarnLevel) {
		return zap.WarnLevel
	}

	if core.Enabled(zap.ErrorLevel) {
		return zap.ErrorLevel
	}

	return zap.PanicLevel
}
