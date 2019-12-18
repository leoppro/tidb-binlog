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

package sync

import (
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/drainer/translator"
	"github.com/pingcap/tidb-binlog/pkg/binlogfile"
	"go.uber.org/zap"
)

var _ Syncer = &ddlSyncer{}

type ddlSyncer struct {
	binlogger binlogfile.Binlogger

	*baseSyncer
}

func NewDDLSyncer(tableInfoGetter translator.TableInfoGetter) (*ddlSyncer, error) {

	s := &ddlSyncer{
		baseSyncer: newBaseSyncer(tableInfoGetter),
	}

	return s, nil
}

func (p *ddlSyncer) Sync(item *Item) error {

	if item.Binlog.DdlJobId != 0 {
		// is ddl
		query := string(item.Binlog.GetDdlQuery())

		log.Info("show ddl", zap.String("query", query),
			zap.Int64("start_ts", item.Binlog.GetStartTs()),
			zap.Int64("commit_ts", item.Binlog.GetCommitTs()),
			zap.Int64("ddl_job_id", item.Binlog.GetDdlJobId()),
		)
	}

	p.success <- item

	return nil
}

func (p *ddlSyncer) Close() error {
	err := p.binlogger.Close()
	p.setErr(err)
	close(p.success)

	return p.err
}
