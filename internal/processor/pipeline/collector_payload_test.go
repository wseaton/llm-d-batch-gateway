/*
Copyright 2026 The llm-d Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/go-logr/logr"

	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
	"github.com/llm-d/llm-d-batch-gateway/pkg/clients/inference"
)

type adoptFunc func(ctx context.Context, item ResultItem) (map[string]any, error)

func (f adoptFunc) Adopt(ctx context.Context, item ResultItem) (map[string]any, error) {
	return f(ctx, item)
}

func refResult(id string) ResultItem {
	return ResultItem{
		RequestID: id,
		CustomID:  "line-" + id,
		Response:  &batch_types.ResponseData{StatusCode: 200, RequestID: id},
		Payload:   &inference.PayloadRef{Ref: "s3://files/async/" + id + "/gen-a", ContentType: "audio/mpeg", Size: 42, SHA256: "ab"},
	}
}

// drainOne runs one result through a collector with the given adopter and returns the
// output and error lines it wrote.
func drainOne(t *testing.T, adopter PayloadAdopter, item ResultItem) ([][]byte, [][]byte) {
	t.Helper()
	outputFile, errorFile := tempFile(t), tempFile(t)
	pending := NewPendingRequests(0)
	c := NewResultCollector(outputFile, errorFile, pending, NewProgressTracker(1, nil, "job", 0, logr.Discard()), logr.Discard())
	if adopter != nil {
		c.SetPayloadAdopter(adopter)
	}
	pending.Store(RequestItem{RequestID: item.RequestID, CustomID: item.CustomID})
	ch := make(chan ResultItem, 1)
	ch <- item
	close(ch)
	if err := c.Drain(context.Background(), ch); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	return splitLines(readFile(t, outputFile)), splitLines(readFile(t, errorFile))
}

func TestCollectorWritesTheAdoptedFileAsTheResponseBody(t *testing.T) {
	var seen ResultItem
	out, errs := drainOne(t, adoptFunc(func(_ context.Context, item ResultItem) (map[string]any, error) {
		seen = item
		return map[string]any{"file_id": "file_1", "content_type": item.Payload.ContentType, "bytes": item.Payload.Size}, nil
	}), refResult("r1"))

	if len(out) != 1 || len(errs) != 0 {
		t.Fatalf("output/error lines = %d/%d, want 1/0", len(out), len(errs))
	}
	if seen.Payload == nil || seen.Payload.Ref != "s3://files/async/r1/gen-a" {
		t.Errorf("adopter got payload %+v", seen.Payload)
	}
	var line outputLine
	if err := json.Unmarshal(out[0], &line); err != nil {
		t.Fatal(err)
	}
	if line.CustomID != "line-r1" || line.Response == nil || line.Response.StatusCode != 200 {
		t.Fatalf("line = %+v", line)
	}
	if line.Response.Body["file_id"] != "file_1" || line.Response.Body["content_type"] != "audio/mpeg" {
		t.Errorf("body = %v", line.Response.Body)
	}
}

func TestCollectorTurnsAMissingObjectIntoPayloadUnavailable(t *testing.T) {
	out, errs := drainOne(t, adoptFunc(func(context.Context, ResultItem) (map[string]any, error) {
		return nil, fmt.Errorf("adopt: %w", os.ErrNotExist)
	}), refResult("r2"))
	assertErrorLine(t, out, errs, "payload_unavailable")
}

func TestCollectorTurnsAFailedAdoptionIntoAnErrorLine(t *testing.T) {
	out, errs := drainOne(t, adoptFunc(func(context.Context, ResultItem) (map[string]any, error) {
		return nil, errors.New("payload ref is not under async/r3/")
	}), refResult("r3"))
	assertErrorLine(t, out, errs, "server_error")
}

func TestCollectorWithoutAnAdopterRejectsReferences(t *testing.T) {
	out, errs := drainOne(t, nil, refResult("r4"))
	assertErrorLine(t, out, errs, "server_error")
}

func assertErrorLine(t *testing.T, out, errs [][]byte, code string) {
	t.Helper()
	if len(out) != 0 || len(errs) != 1 {
		t.Fatalf("output/error lines = %d/%d, want 0/1", len(out), len(errs))
	}
	var line outputLine
	if err := json.Unmarshal(errs[0], &line); err != nil {
		t.Fatal(err)
	}
	if line.Error == nil || line.Error.Code != code || line.Response != nil {
		t.Errorf("line = %+v, want error %s and no response", line, code)
	}
}
