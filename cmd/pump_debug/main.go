// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	_ "net/http/pprof"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/pkg/util"
	"github.com/pingcap/tidb-binlog/pkg/version"
	"github.com/pingcap/tidb-binlog/pump"
	"github.com/pingcap/tidb-binlog/pump/storage"
	"go.uber.org/zap"
	_ "google.golang.org/grpc/encoding/gzip"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	rand.Seed(time.Now().UTC().UnixNano())

	cfg := pump.NewConfig()
	if err := cfg.Parse(os.Args[1:]); err != nil {
		log.Fatal("verifying flags failed. See 'pump --help'.", zap.Error(err))
	}

	if err := util.InitLogger(cfg.LogLevel, cfg.LogFile); err != nil {
		log.Fatal("Failed to initialize log", zap.Error(err))
	}
	version.PrintVersionInfo("Pump Debug")

	s, err := storage.NewAppend(cfg.DataDir, storage.DefaultOptions())
	if err != nil {
		log.Fatal("Error", zap.Error(err))
	}
	s.PullCommitBinlogForDebug(context.Background(), 0)

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	var wg sync.WaitGroup

	go func() {
		sig := <-sc
		log.Info("got signal to exit.", zap.Stringer("signal", sig))
		wg.Add(1)
		s.Close()
		log.Info("pump is closed")
		wg.Done()
	}()

	wg.Wait()
	log.Info("pump exit")
}
