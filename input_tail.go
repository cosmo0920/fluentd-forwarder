//
// Fluentd Forwarder
//
// Copyright (C) 2015 Treasure Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package fluentd_forwarder

import (
	"bufio"
	"fmt"
	logging "github.com/op/go-logging"
	"github.com/ugorji/go/codec"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"time"
	"gopkg.in/fsnotify.v1"
)

const (
	TailInterval = 100 * time.Millisecond
)

type forwardTailClient struct {
	input  *ForwardInput
	logger *logging.Logger
	conn   *net.TCPConn
	codec  *codec.MsgpackHandle
	dec    *codec.Decoder
}

type Watcher struct {
	watcher      *fsnotify.Watcher
	watchingDir  map[string]bool
	watchingFile map[string]chan fsnotify.Event
}

type ForwardInputTail struct {
	entries        int64 // This variable must be on 64-bit alignment. Otherwise atomic.AddInt64 will cause a crash on ARM and x86-32
	filename       string
	logger         *logging.Logger
	lastReadAt     time.Time
	codec          *codec.MsgpackHandle
	clientsMtx     sync.Mutex
	clients        map[*net.TCPConn]*forwardClient
	wg             sync.WaitGroup
	eventCh        chan fsnotify.Event
	// monitorCh      chan MonitorStat // TODO: implement
	shutdownChan   chan struct{}
	isShuttingDown uintptr
}

func (input *ForwardInput) NewWatcher() (*Watcher, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		input.logger.Error("Could not create file watcher")
		return nil, err
	}
	w := &Watcher{
		watcher:      watcher,
		watchingDir:  make(map[string]bool),
		watchingFile: make(map[string]chan fsnotify.Event),
	}
	return w, nil
}

func (w *Watcher) Run() {
	if len(w.watchingFile) == 0 {
		//input.logger.Error("No watching file. Watcher aborted.")
		return
	}
	for {
		select {
		case ev := <- w.watcher.Events:
			if eventCh, ok := w.watchingFile[ev.Name]; ok {
				eventCh <- ev
			}
		case _ = <- w.watcher.Errors:
			//input.logger.Warning("Watcher error.", err)
		}
	}
}

func (w *Watcher) WatchFile(filename string, logger *logging.Logger) (chan fsnotify.Event, error) {
	parent := filepath.Dir(filename)
	logger.Info("Watching events of directory")
	if _, ok := w.watchingDir[parent]; ok {
		ch := make(chan fsnotify.Event)
		w.watchingFile[filename] = ch
		return ch, nil
	} else {
		err := w.watcher.Add(parent)
		if err != nil {
			logger.Error("Could not watch event %v %v", parent, err)
			return nil, err
		}
		w.watchingDir[parent] = true
		ch := make(chan fsnotify.Event)
		w.watchingFile[filename] = ch
		return ch, nil
	}
}

func RelativeToAbsolute(filename string) (string, error) {
	if filepath.IsAbs(filename) {
		return filename, nil
	}
	cwd, err := os.Getwd()
	if err != nil {
		fmt.Errorf("Could not get current working dir. %v", err)
		return "", err
	}
	return filepath.Join(cwd, filename), nil
}

func newForwardTailClient(input *ForwardInput, logger *logging.Logger, conn *net.TCPConn, _codec *codec.MsgpackHandle) *forwardClient {
	c := &forwardClient{
		input:  input,
		logger: logger,
		conn:   conn,
		codec:  _codec,
		dec:    codec.NewDecoder(bufio.NewReader(conn), _codec),
	}
	input.markCharged(c)
	return c
}

func NewForwardInputTail(logger *logging.Logger, watcher *Watcher) (*ForwardInputTail, error) {
	_codec := codec.MsgpackHandle{}
	_codec.MapType = reflect.TypeOf(map[string]interface{}(nil))
	_codec.RawToString = false
	filename, err := RelativeToAbsolute("config.File")
	if err != nil {
		logger.Error("%s", err.Error())
		return nil, err
	}
	eventCh, err := watcher.WatchFile(filename, logger)
	if err != nil {
		logger.Error("%s", err.Error())
		return nil, err
	}
	return &ForwardInputTail{
		filename:       filename,
		logger:         logger,
		lastReadAt:     time.Now(),
		codec:          &_codec,
		clients:        make(map[*net.TCPConn]*forwardClient),
		clientsMtx:     sync.Mutex{},
		entries:        0,
		wg:             sync.WaitGroup{},
		eventCh:        eventCh,
		// monitorCh:      monitorCh, // TODO: implement
		shutdownChan:   make(chan struct{}),
		isShuttingDown: uintptr(0),
	}, nil
}

func (t *ForwardInputTail) Run() {
	defer t.logger.Error("Aborted to input_tail.run()")

	t.logger.Info("Trying tail file %v", t.filename)
	// f := t.newTailFile(SEEK_TAIL)
}

// TODO: create File util and adopt this
// func (t *ForwardInputTail) newTailFile(startPos int64) *File {
// 	seekTo := startPos
// 	first := true
// 	for {
// 		f, err := openFile(t.filename, seekTo)
// 		if err == nil {
// 			t.logger.Info("Tail File:", f.Path)
// 		}
// 		// TODO: monitor file stat
// 		first = false
// 		seekTo = SEEK_HEAD
// 		time.Sleep(OpenRetryInterval)
// 	}
// }
