package api

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestRefResultSurvivesTheWireFormat(t *testing.T) {
	req := &RequestMessage{ID: "speech-1", Metadata: map[string]string{"k": "v"}}
	msg := NewHTTPRefResult(req, InternalRouting{RequestToken: "gen-a"}, 200, "s3://results/p/speech-1/gen-a", "audio/mpeg", 240000, "ab12")

	data, err := json.Marshal(InternalResult{ResultMessage: msg, RequestToken: msg.Routing.RequestToken})
	if err != nil {
		t.Fatal(err)
	}
	var got InternalResult
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.PayloadRef != msg.PayloadRef || got.ContentType != "audio/mpeg" || got.PayloadSize != 240000 || got.PayloadSHA256 != "ab12" || got.StatusCode != 200 {
		t.Errorf("round trip lost reference fields: %+v", got.ResultMessage)
	}
	if got.Payload != "" {
		t.Errorf("reference result carries an inline payload %q", got.Payload)
	}
}

func TestInlineResultWireFormatIsUnchanged(t *testing.T) {
	data, err := json.Marshal(NewHTTPResult(&RequestMessage{ID: "chat-1"}, InternalRouting{}, 200, []byte(`{"ok":true}`)))
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"payload_ref", "content_type", "payload_size", "payload_sha256"} {
		if strings.Contains(string(data), field) {
			t.Errorf("inline result gained %q: %s", field, data)
		}
	}
}
