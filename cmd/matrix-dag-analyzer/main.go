// Copyright 2023 The Matrix.org Foundation C.I.C.
//
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

package main

import (
	"errors"
	"flag"
	"os"
	"time"

	"github.com/matrix-org/matrix-dag-analyzer"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{
		Out:        os.Stderr,
		TimeFormat: time.RFC3339,
	}).With().Caller().Logger()

	var filePath string
	flag.StringVar(&filePath, "p", "", "Specify dag file name with path")
	flag.Parse()

	if len(filePath) < 1 {
		log.Error().
			Err(errors.New("Invalid file path specified")).
			Msg("")
		flag.Usage()
		return
	}

	dag, err := analyzer.ParseDAGFromFile(filePath)
	if err != nil {
		log.Error().Err(err).Msg("failed parsing file")
		return
	}

	log.Info().
		Int("event_count", dag.TotalEventCount()).
		Int("create_count", dag.EventCountByType(analyzer.EVENT_TYPE_CREATE)).
		Msg("parsed dag file")
	dag.PrintEventCounts()
}
