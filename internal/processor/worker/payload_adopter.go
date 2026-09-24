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

import (
	"context"
	"fmt"
	"mime"
	"net/url"
	"path"
	"strings"

	db "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	filesapi "github.com/llm-d/llm-d-batch-gateway/internal/files_store/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/pipeline"
	ucom "github.com/llm-d/llm-d-batch-gateway/internal/util/com"
)

var _ pipeline.PayloadAdopter = (*jobPayloadAdopter)(nil)

// jobPayloadAdopter adopts one job's stored-by-reference response bodies as batch_output
// files owned by the job's tenant.
type jobPayloadAdopter struct {
	p         *Processor
	files     filesapi.ObjectAdopter
	tenantID  string
	folder    string
	batchTags db.Tags
	keyPrefix string
}

func (a *jobPayloadAdopter) Adopt(ctx context.Context, item pipeline.ResultItem) (map[string]any, error) {
	ref := item.Payload
	if err := checkPayloadRef(ref.Ref, a.keyPrefix, item.RequestID); err != nil {
		return nil, err
	}
	fileID := ucom.NewFileID()
	fileName := payloadFileName(item.CustomID, item.RequestID, ref.ContentType)
	if _, err := a.files.Adopt(ctx, ref.Ref, ucom.FileStorageName(fileID, fileName), a.folder); err != nil {
		return nil, err
	}
	if err := a.p.storeFileRecord(ctx, fileID, fileName, a.tenantID, ref.Size, a.batchTags); err != nil {
		return nil, fmt.Errorf("record adopted file %s: %w", fileID, err)
	}
	return map[string]any{
		"file_id":      fileID,
		"content_type": ref.ContentType,
		"bytes":        ref.Size,
		"sha256":       ref.SHA256,
	}, nil
}

// checkPayloadRef accepts only s3://<bucket>/<prefix>/<request id>[/<token>], so a result can
// only point the gateway at the object stored for its own request.
func checkPayloadRef(ref, keyPrefix, requestID string) error {
	rest, ok := strings.CutPrefix(ref, "s3://")
	if !ok {
		return fmt.Errorf("payload ref %q is not an s3:// reference", ref)
	}
	_, key, ok := strings.Cut(rest, "/")
	if !ok {
		return fmt.Errorf("payload ref %q has no object key", ref)
	}
	want := path.Join(keyPrefix, url.PathEscape(requestID))
	if key != want && !strings.HasPrefix(key, want+"/") {
		return fmt.Errorf("payload ref %q is not under %s/ for request %s", ref, want, requestID)
	}
	return nil
}

var payloadExtensions = map[string]string{
	"audio/mpeg":      ".mp3",
	"audio/mp3":       ".mp3",
	"audio/wav":       ".wav",
	"audio/x-wav":     ".wav",
	"audio/wave":      ".wav",
	"audio/flac":      ".flac",
	"audio/ogg":       ".ogg",
	"audio/opus":      ".opus",
	"audio/aac":       ".aac",
	"audio/pcm":       ".pcm",
	"audio/L16":       ".pcm",
	"image/png":       ".png",
	"image/jpeg":      ".jpg",
	"image/webp":      ".webp",
	"video/mp4":       ".mp4",
	"text/plain":      ".txt",
	"text/csv":        ".csv",
	"application/pdf": ".pdf",
}

// payloadFileName names an adopted file after the input line's custom_id (or the request ID),
// keeping only characters safe in a file name, with an extension for its media type.
func payloadFileName(customID, requestID, contentType string) string {
	base := customID
	if base == "" {
		base = requestID
	}
	var b strings.Builder
	for _, r := range base {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == '.':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	ext := ".bin"
	if mediaType, _, err := mime.ParseMediaType(contentType); err == nil {
		if e, ok := payloadExtensions[mediaType]; ok {
			ext = e
		}
	}
	return b.String() + ext
}
