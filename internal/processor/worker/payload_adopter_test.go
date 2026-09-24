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

package worker

import "testing"

func TestCheckPayloadRef(t *testing.T) {
	for _, tc := range []struct {
		ref, prefix, requestID string
		ok                     bool
	}{
		{"s3://files/llm-d-async/results/req-1/gen-a", "llm-d-async/results", "req-1", true},
		{"s3://files/llm-d-async/results/req-1", "llm-d-async/results", "req-1", true},
		{"s3://files/req-1/gen-a", "", "req-1", true},
		{"s3://files/llm-d-async/results/batch%207/gen-a", "llm-d-async/results", "batch 7", true},
		{"s3://files/llm-d-async/results/req-10/gen-a", "llm-d-async/results", "req-1", false},
		{"s3://files/llm-d-async/results/req-2/gen-a", "llm-d-async/results", "req-1", false},
		{"s3://files/other/req-1/gen-a", "llm-d-async/results", "req-1", false},
		{"s3://files/tenant-b/file_x_c.jsonl", "", "req-1", false},
		{"https://files/llm-d-async/results/req-1", "llm-d-async/results", "req-1", false},
		{"s3://files", "", "req-1", false},
	} {
		err := checkPayloadRef(tc.ref, tc.prefix, tc.requestID)
		if (err == nil) != tc.ok {
			t.Errorf("checkPayloadRef(%q, %q, %q) = %v, want ok=%v", tc.ref, tc.prefix, tc.requestID, err, tc.ok)
		}
	}
}

func TestPayloadFileName(t *testing.T) {
	for _, tc := range []struct{ customID, requestID, contentType, want string }{
		{"chapter-1", "req-1", "audio/mpeg", "chapter-1.mp3"},
		{"chapter 1/intro", "req-1", "audio/wav", "chapter_1_intro.wav"},
		{"", "req-9", "audio/flac", "req-9.flac"},
		{"x", "req-1", "audio/mpeg; charset=binary", "x.mp3"},
		{"x", "req-1", "application/x-unknown", "x.bin"},
		{"x", "req-1", "", "x.bin"},
	} {
		if got := payloadFileName(tc.customID, tc.requestID, tc.contentType); got != tc.want {
			t.Errorf("payloadFileName(%q, %q, %q) = %q, want %q", tc.customID, tc.requestID, tc.contentType, got, tc.want)
		}
	}
}
