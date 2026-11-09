//go:build integration

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

package integration

import (
	"bytes"
	"context"
	"errors"
	"io"
	"slices"
	"testing"

	"github.com/google/uuid"
	"github.com/llm-d/llm-d-batch-gateway/internal/files_store/api"
	s3client "github.com/llm-d/llm-d-batch-gateway/internal/files_store/s3"
)

// streamPayload returns size bytes of JSONL-shaped content unique to seed.
func streamPayload(size int, seed byte) []byte {
	line := append(bytes.Repeat([]byte{'a' + seed%26}, 1023), '\n')
	return bytes.Repeat(line, size/len(line))
}

// objectCount returns how many objects exist under folder/dir/, including
// any staging leftovers, by deleting them.
func objectCount(t *testing.T, client *s3client.Client, folder, dir string) int {
	t.Helper()
	n, err := client.DeletePrefix(context.Background(), folder, dir)
	if err != nil {
		t.Fatalf("DeletePrefix: %v", err)
	}
	return n
}

func writeStream(t *testing.T, w api.StreamWriter, data []byte) {
	t.Helper()
	for chunk := range slices.Chunk(data, 256<<10) {
		if _, err := w.Write(chunk); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
}

func TestS3OpenStream(t *testing.T) {
	client := newS3IntegrationClient(t, s3Config())
	ctx := context.Background()

	sizes := []struct {
		name string
		size int
	}{
		{name: "single part", size: 64 << 10},
		{name: "multipart", size: 12 << 20},
	}

	for _, sz := range sizes {
		t.Run(sz.name+"/commit publishes the bytes", func(t *testing.T) {
			folder := "stream-" + uuid.NewString()[:8]
			data := streamPayload(sz.size, 1)
			w, err := client.OpenStream(ctx, "out/output.jsonl", folder)
			if err != nil {
				t.Fatalf("OpenStream: %v", err)
			}
			writeStream(t, w, data)
			md, err := w.Commit(ctx)
			if err != nil {
				t.Fatalf("Commit: %v", err)
			}
			if md.Size != int64(len(data)) {
				t.Errorf("metadata size = %d, want %d", md.Size, len(data))
			}

			r, rmd, err := client.Retrieve(ctx, "out/output.jsonl", folder)
			if err != nil {
				t.Fatalf("Retrieve: %v", err)
			}
			got, err := io.ReadAll(r)
			_ = r.Close()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, data) || rmd.Size != int64(len(data)) {
				t.Fatalf("retrieved %d bytes (size %d), want the %d committed bytes", len(got), rmd.Size, len(data))
			}
			if n := objectCount(t, client, folder, "out"); n != 1 {
				t.Errorf("%d objects under the folder after commit, want only the published one", n)
			}
		})

		t.Run(sz.name+"/concurrent writers publish exactly once", func(t *testing.T) {
			folder := "stream-" + uuid.NewString()[:8]
			first, second := streamPayload(sz.size, 2), streamPayload(sz.size, 3)
			w1, err := client.OpenStream(ctx, "out/output.jsonl", folder)
			if err != nil {
				t.Fatalf("OpenStream 1: %v", err)
			}
			w2, err := client.OpenStream(ctx, "out/output.jsonl", folder)
			if err != nil {
				t.Fatalf("OpenStream 2: %v", err)
			}
			writeStream(t, w1, first)
			writeStream(t, w2, second)
			if _, err := w1.Commit(ctx); err != nil {
				t.Fatalf("first Commit: %v", err)
			}
			if _, err := w2.Commit(ctx); !errors.Is(err, api.ErrFileExists) {
				t.Fatalf("second Commit = %v, want ErrFileExists", err)
			}

			r, _, err := client.Retrieve(ctx, "out/output.jsonl", folder)
			if err != nil {
				t.Fatalf("Retrieve: %v", err)
			}
			got, err := io.ReadAll(r)
			_ = r.Close()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, first) {
				t.Fatalf("published object is not the first writer's bytes (len %d)", len(got))
			}
			if n := objectCount(t, client, folder, "out"); n != 1 {
				t.Errorf("%d objects under the folder, want only the published one", n)
			}
		})

		t.Run(sz.name+"/abort leaves nothing", func(t *testing.T) {
			folder := "stream-" + uuid.NewString()[:8]
			w, err := client.OpenStream(ctx, "out/output.jsonl", folder)
			if err != nil {
				t.Fatalf("OpenStream: %v", err)
			}
			writeStream(t, w, streamPayload(sz.size, 4))
			if err := w.Abort(ctx); err != nil {
				t.Fatalf("Abort: %v", err)
			}
			if _, _, err := client.Retrieve(ctx, "out/output.jsonl", folder); err == nil {
				t.Fatal("aborted stream is retrievable")
			}
			if n := objectCount(t, client, folder, "out"); n != 0 {
				t.Errorf("%d objects under the folder after abort, want 0", n)
			}
		})
	}

	t.Run("open on an existing key fails", func(t *testing.T) {
		folder := "stream-" + uuid.NewString()[:8]
		t.Cleanup(func() { objectCount(t, client, folder, "out") })
		if _, err := client.Store(ctx, "out/output.jsonl", folder, 1024, 0, bytes.NewReader([]byte("x\n"))); err != nil {
			t.Fatalf("Store: %v", err)
		}
		if _, err := client.OpenStream(ctx, "out/output.jsonl", folder); !errors.Is(err, api.ErrFileExists) {
			t.Fatalf("OpenStream = %v, want ErrFileExists", err)
		}
	})
}
