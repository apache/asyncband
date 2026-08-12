// Copyright 2024 tison <wander4096@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! A multi-producer, single-consumer queue for sending values between asynchronous tasks.

mod bounded;
mod error;
#[cfg(test)]
mod tests;
mod unbounded;

pub use bounded::BoundedReceiver;
pub use bounded::BoundedSender;
pub use bounded::bounded;
pub use error::RecvError;
pub use error::SendError;
pub use error::TryRecvError;
pub use error::TrySendError;
pub use unbounded::UnboundedReceiver;
pub use unbounded::UnboundedSender;
pub use unbounded::unbounded;
