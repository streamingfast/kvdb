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

	"github.com/sirupsen/logrus"
	"github.com/streamingfast/logging"
	"github.com/tikv/client-go/v2/config"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var hook = &logrusHook{}

var zlog, tracer = logging.PackageLogger("kvdb", "github.com/streamingfast/kvdb/store/tikv", logging.LoggerOnUpdate(func(newLogger *zap.Logger) {
	hook.logger = newLogger.Named("tikv-client")
	reconfigureLogrusLevel(hook.logger)
}))

func init() {
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

type tikvConfigRaw config.TiKVClient

func (c tikvConfigRaw) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	v := config.TiKVClient(c)

	enc.AddInt("max_batch_size", int(v.MaxBatchSize))
	enc.AddInt("max_batch_wait_time", int(v.MaxBatchWaitTime))
	enc.AddInt("batch_wait_size", int(v.BatchWaitSize))
	enc.AddBool("enable_chunk_rpc", v.EnableChunkRPC)
	enc.AddString("grpc_compression_type", v.GrpcCompressionType)
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
