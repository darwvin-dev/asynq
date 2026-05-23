// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package typed

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/hibiken/asynq"
)

type testPayload struct {
	To      string `json:"to"`
	Message string `json:"message"`
}

func TestRegisterDecodesPayload(t *testing.T) {
	mux := asynq.NewServeMux()
	want := testPayload{To: "+15550000000", Message: "hello"}
	var got testPayload
	Register(mux, "sms.send", func(_ context.Context, payload testPayload) error {
		got = payload
		return nil
	})
	data, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	if err := mux.ProcessTask(context.Background(), asynq.NewTask("sms.send", data)); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("payload = %+v, want %+v", got, want)
	}
}

func TestRegisterReturnsDecodeError(t *testing.T) {
	mux := asynq.NewServeMux()
	Register(mux, "sms.send", func(_ context.Context, payload testPayload) error {
		return nil
	})
	if err := mux.ProcessTask(context.Background(), asynq.NewTask("sms.send", []byte("{"))); err == nil {
		t.Fatal("ProcessTask returned nil, want decode error")
	}
}
