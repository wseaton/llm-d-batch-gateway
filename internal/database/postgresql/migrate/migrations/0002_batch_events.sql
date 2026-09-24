-- Copyright 2026 The llm-d Authors
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at

-- http://www.apache.org/licenses/LICENSE-2.0

-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Durable-until-consumed events (BatchEventChannelClient).
-- id BIGSERIAL gives FIFO ordering. event_type = int(BatchEventType). expires_at = unix seconds.
-- Rows are the source of truth (durable, late-attach-safe); NOTIFY is a latency hint.
CREATE TABLE IF NOT EXISTS batch_events (
    id         BIGSERIAL PRIMARY KEY,
    job_id     TEXT      NOT NULL,
    event_type INTEGER   NOT NULL,
    expires_at BIGINT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_batch_events_job_id ON batch_events (job_id, id);
CREATE INDEX IF NOT EXISTS idx_batch_events_expires_at ON batch_events (expires_at);
