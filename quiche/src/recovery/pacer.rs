// Copyright (C) 2022, Cloudflare, Inc.
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//
//     * Redistributions in binary form must reproduce the above copyright
//       notice, this list of conditions and the following disclaimer in the
//       documentation and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
// IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
// THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
// PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
// EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
// PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
// PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
// LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

//! Pacer provdes the timestamp for next packet to be sent
//! based on the current send_quantum, pacing rate and last
//! updated time. It's a kind of leaky bucket algorithm
//! (RFC 9002 7.7 Pacing) but it considers max burst
//! (send_quantum, in bytes). It means now a groups of
//! packets within send_quantum will have a same timestamp
//! (assuming we send packets using multiple sendmsg(),
//! a sendmmsg(), or sendmsg() with GSO without waiting for
//! new I/O event). After sending a burst of packets,
//! next timestamp will be updated based on the current
//! pacing rate. It will make actual timestamp sent and
//! recorded timestamp (Sent.time_sent) is
//! close as much as possible.

use std::time::Duration;
use std::time::Instant;

#[derive(Debug)]
pub struct Pacer {
    // Bucket capacity (bytes).
    capacity: usize,

    // Bucket used (bytes).
    used: usize,

    // Sending pacing rate (bytes/sec).
    rate: u64,

    // Timestamp of last packet sent time update.
    last_update: Instant,

    // Timestamp of next packet to be sent.
    next_time: Instant,
}

impl Pacer {
    pub fn new(capacity: usize, rate: u64) -> Self {
        Pacer {
            capacity,

            used: 0,

            rate,

            last_update: Instant::now(),

            next_time: Instant::now(),
        }
    }

    // Update bucket capacity or pacing_rate.
    pub fn update(&mut self, capacity: usize, rate: u64) {
        self.capacity = capacity;

        self.rate = rate;

        self.reset();
    }

    // Reset pacer for next burst.
    pub fn reset(&mut self) {
        self.used = 0;

        let now = Instant::now();

        self.last_update = now;

        self.next_time = self.next_time.max(now);
    }

    // Update the timestamp to sent.
    pub fn send(&mut self, sent_bytes: usize, now: Instant) {
        if self.rate == 0 || sent_bytes == 0 {
            self.next_time = self.last_update.max(now);
            self.last_update = self.next_time;

            return;
        }

        let interval =
            Duration::from_secs_f64(self.capacity as f64 / self.rate as f64);
        let elapsed = now.saturating_duration_since(self.last_update);

        // if too old, reset it.
        if elapsed > interval {
            self.reset();
        }

        self.used += sent_bytes;

        let next = if self.used >= self.capacity {
            self.used -= self.capacity;
            self.last_update = now;

            interval
        } else {
            Duration::ZERO
        };

        self.next_time = (self.last_update + next).max(now);
    }

    // Returns the timestamp to send a next packet.
    pub fn next_time(&self) -> Instant {
        self.next_time
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pacer_update() {
        let max_burst = 12000;
        let pacing_rate = 100_000;

        let mut p = Pacer::new(max_burst, pacing_rate);

        let now = Instant::now();

        // send 6000 (a half of max_burst) -> no timestamp change yet
        p.send(6000, now);

        assert_eq!(p.next_time(), now);

        // send 6000 bytes -> max_burst filled, next time will be updated
        p.send(6000, now);

        let interval = max_burst as f64 / pacing_rate as f64;

        assert_eq!(p.next_time() - now, Duration::from_secs_f64(interval));

        let now = now + Duration::from_millis(1);

        // send 1000 bytes -> new burst started
        p.send(1000, now);

        assert_eq!(p.next_time(), now);
    }

    #[test]
    fn pacer_idle() {
        // same as pacer_update() but insert some idleness
        // between two transfer, causing resetting
        let max_burst = 12000;
        let pacing_rate = 100_000;

        let mut p = Pacer::new(max_burst, pacing_rate);

        let now = Instant::now();

        // send 6000 (a half of max_burst) -> no timestamp change yet
        p.send(6000, now);

        assert_eq!(p.next_time(), now);

        // sleep 200ms to reset the idle pacer (at least 120ms).
        let now = now + Duration::from_millis(200);

        // send 6000 bytes -> idle reset and a new burst started
        p.send(6000, now);

        assert_eq!(p.next_time(), now);
    }
}
