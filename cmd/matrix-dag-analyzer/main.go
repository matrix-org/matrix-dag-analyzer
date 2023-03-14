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
	zerolog.TimeFieldFormat = time.RFC3339Nano
	log.Logger = log.Output(zerolog.ConsoleWriter{
		Out:        os.Stderr,
		TimeFormat: "15:04:05.000",
	})

	var filePath string
	var outputFilePath string
	flag.StringVar(&filePath, "p", "", "Specify dag file name with path")
	flag.StringVar(&outputFilePath, "o", "", "Specify output power DAG file name with path")
	flag.Parse()

	if len(filePath) < 2 {
		log.Error().
			Err(errors.New("Invalid file path specified")).
			Msg("")
		flag.Usage()
		return
	}
	if len(outputFilePath) < 2 {
		log.Error().
			Err(errors.New("Invalid output file path specified")).
			Msg("")
		flag.Usage()
		return
	}

	dag, err := analyzer.ParseDAGFromFile(filePath, outputFilePath)
	if err != nil {
		log.Error().Err(err).Msg("failed parsing file")
		return
	}

	log.Info().
		Int("events_in_file", dag.EventsInFile()).
		Int("events_referenced", dag.TotalEvents()).
		Int("create_count", dag.EventCountByType(analyzer.EVENT_TYPE_CREATE)).
		Msg("Successfully parsed dag file")
	if dag.TotalEvents() != dag.EventsInFile() {
		log.Warn().
			Int("events_in_file", dag.EventsInFile()).
			Int("events_referenced", dag.TotalEvents()).
			Int("delta", dag.TotalEvents()-dag.EventsInFile()).
			Msg("The number of events in the file does not match the number of events referenced in the file")
	}
	dag.PrintMetrics()

	// TODO: Create new `Power Events` & write to json file
}
