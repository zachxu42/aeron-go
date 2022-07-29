// Copyright (C) 2021-2022 Talos, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// An example replayed subscriber
package main

import (
	"flag"
	"fmt"
	"github.com/corymonroe-coinbase/aeron-go/aeron"
	"github.com/corymonroe-coinbase/aeron-go/aeron/atomic"
	"github.com/corymonroe-coinbase/aeron-go/aeron/idlestrategy"
	"github.com/corymonroe-coinbase/aeron-go/aeron/logbuffer"
	"github.com/corymonroe-coinbase/aeron-go/aeron/logging"
	"github.com/corymonroe-coinbase/aeron-go/archive"
	"github.com/corymonroe-coinbase/aeron-go/archive/examples"
	"math"
	"time"
)

var logID = "basic_recording_subscriber"
var logger = logging.MustGetLogger(logID)

// public static final int SESSION_HEADER_LENGTH = MessageHeaderEncoder.ENCODED_LENGTH + SessionMessageHeaderEncoder.BLOCK_LENGTH;
var session_header_length = int32(32)

func main() {
	flag.Parse()

	replayChannel := *examples.Config.ReplayChannel
	replayStream := int32(*examples.Config.ReplayStream)

	timeout := time.Duration(time.Millisecond.Nanoseconds() * *examples.Config.DriverTimeout)
	context := aeron.NewContext()
	context.AeronDir(*examples.Config.AeronPrefix)
	context.MediaDriverTimeout(timeout)

	options := archive.DefaultOptions()
	options.RequestChannel = *examples.Config.RequestChannel
	options.RequestStream = int32(*examples.Config.RequestStream)
	options.ResponseChannel = *examples.Config.ResponseChannel

	if *examples.Config.Verbose {
		fmt.Printf("Setting loglevel: archive.DEBUG/aeron.INFO\n")
		options.ArchiveLoglevel = logging.DEBUG
		options.AeronLoglevel = logging.DEBUG
		logging.SetLevel(logging.DEBUG, logID)
	} else {
		logging.SetLevel(logging.NOTICE, logID)
	}

	arch, err := archive.NewArchive(options, context)
	if err != nil {
		logger.Fatalf("Failed to connect to media driver: %s\n", err.Error())
	}
	defer arch.Close()

	// Enable recording events although the defaults will only log in debug mode
	arch.EnableRecordingEvents()

	logRecordingId, err := FindLogRecording(arch)
	if err != nil {
		logger.Fatalf(err.Error())
	}

	idleStrategy := idlestrategy.Sleeping{SleepFor: time.Millisecond * 1}

	recordingStopPosition, err := arch.GetStopPosition(logRecordingId)
	if err != nil {
		logger.Fatalf(err.Error())
	}

	if recordingStopPosition != archive.RecordingPositionNull {
		logger.Fatalf("Recording is not active")
	}

	replaySessionID, err := arch.StartReplay(logRecordingId, 0, math.MaxInt, replayChannel, replayStream)

	if err != nil {
		logger.Fatalf(err.Error())
	}

	// Make the channel based upon that recording and subscribe to it
	subChannel, err := archive.AddSessionIdToChannel(replayChannel, archive.ReplaySessionIdToStreamId(replaySessionID))
	if err != nil {
		logger.Fatalf("AddReplaySessionIdToChannel() failed: %s", err.Error())
	}

	logger.Infof("Subscribing to channel:%s, stream:%d", subChannel, replayStream)
	subscription := <-arch.AddSubscription(subChannel, replayStream)
	defer subscription.Close()
	logger.Infof("Subscription found %v", subscription)

	printHandler := func(buffer *atomic.Buffer, offset int32, length int32, header *logbuffer.Header) {
		templateId := buffer.GetUInt16(offset + 2)
		if templateId == 1 {
			// Skip 32 bytes for Session Messages, which is templateId = 1
			fmt.Println(buffer.GetBytesArray(offset+session_header_length, length-session_header_length))
		}
	}

	for {
		fragmentsRead := subscription.Poll(printHandler, 10)
		arch.RecordingEventsPoll()
		idleStrategy.Idle(fragmentsRead)
	}
}

// FindLogRecording to lookup the log recording
func FindLogRecording(arch *archive.Archive) (int64, error) {
	descriptors, err := arch.ListRecordingsForUri(0, 10, "log", 100)
	if len(descriptors) > 1 {
		panic("Log recording should have only a single record")
	}
	if err != nil {
		return 0, err
	}

	if len(descriptors) == 0 {
		return 0, fmt.Errorf("no recordings found")
	}

	// Return the last recordingID
	return descriptors[0].RecordingId, nil
}
