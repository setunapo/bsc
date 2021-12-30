// Copyright 2016 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

//go:build go1.5
// +build go1.5

package debug

import (
	"context"
	"errors"
	"os"
	"runtime/trace"

	"github.com/ethereum/go-ethereum/log"
)

// StartGoTrace turns on tracing, writing to the given file.
// only do file open
func (h *HandlerT) StartGoTrace(file string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.traceW != nil {
		return errors.New("trace file already opened")
	}

	if h.traceFile == "" {
		h.traceFile = file
	} else {
		h.traceFile += "_"
	}
	f, err := os.Create(expandHome(h.traceFile))
	if err != nil {
		log.Info("StartGoTrace file created", "file", h.traceFile, "err", err)
		return err
	}

	h.traceW = f

	log.Info("StartGoTrace file created", "file", h.traceFile)
	/*
		if err := trace.Start(f); err != nil {
			f.Close()
			return err
		}
		h.ctx, h.task = trace.NewTask(context.Background(), "larryDebugTask")
	*/
	return nil
}

// user controled start & stop capture
func (h *HandlerT) RpcEnableTraceCapture() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	// already running
	if h.task != nil {
		log.Info("trace task is already running")
		return nil
	}

	// create file
	h.mu.Unlock()
	h.StartGoTrace("")
	h.mu.Lock()
	f := h.traceW
	if err := trace.Start(f); err != nil {
		f.Close()
		log.Error("EnableTrace Start failed", "err", err)
		h.traceW = nil
		return err
	}

	h.ctx, h.task = trace.NewTask(context.Background(), "larryDebugTask")
	log.Info("Go tracing started")
	return nil
}

// StopTrace stops an ongoing trace.
func (h *HandlerT) RpcDisableTraceCapture() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.traceW == nil {
		return errors.New("trace not in progress")
	}

	if h.task == nil {
		log.Error("StopGoTrace task is nil!")
	} else {
		h.task.End()
	}
	h.task = nil

	trace.Stop()
	log.Info("Done writing Go trace", "dump", h.traceFile)
	h.traceW.Close()
	h.traceW = nil
	return nil
}

func (h *HandlerT) RpcEnableTraceCaptureWithBlockRange(number, length uint64) {
	h.startBlockNum = number
	h.endBlockNum = number + length
	log.Info("enable traceCapture", "startBlockNum", h.startBlockNum,
		"endBlockNum", h.endBlockNum)
}

// enable a trace capture cycle, with length captureBlockNum
func (h *HandlerT) EnableTraceCapture(blockNum uint64) {
	if blockNum >= h.startBlockNum && blockNum < h.endBlockNum {
		h.RpcEnableTraceCapture()
		return
	}

	h.RpcDisableTraceCapture()
}

func (h *HandlerT) Ctx() context.Context {
	return h.ctx
}

func (h *HandlerT) Task() *trace.Task {
	return h.task
}

// StopTrace stops an ongoing trace.
func (h *HandlerT) StopGoTrace() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.traceW == nil {
		return errors.New("trace not in progress")
	}

	if h.task == nil {
		log.Error("StopGoTrace task is nil!")
	} else {
		h.task.End()
	}
	h.task = nil

	trace.Stop()
	log.Info("Done writing Go trace", "dump", h.traceFile)
	h.traceW.Close()
	h.traceW = nil
	return nil
}

//
func (h *HandlerT) LogWhenTracing(msg string) {
	if h.task == nil {
		return
	}
	log.Info("LogWhenTracing", "msg", msg)
}

func (h *HandlerT) StartRegionAuto(msg string) func() {
	// log.Info("HandlerT StartRegion enter", "msg", msg)
	if h.task == nil {
		return func() {
			// log.Info("HandlerT StartRegion exit not started")
		}
	}

	// task ready, do trace
	// log.Info("StartRegionAuto enter", "msg", msg)
	region := trace.StartRegion(h.ctx, msg)
	return func() {
		// log.Info("StartRegionAuto exit", "msg", msg)
		region.End()
	}
}
