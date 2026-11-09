// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

pub fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

#[cfg(any(feature = "once-map", feature = "singleflight"))]
pub use self::key_with_drop::KeyWithDrop;

#[cfg(any(feature = "once-map", feature = "singleflight"))]
mod key_with_drop {
    use std::borrow::Borrow;
    use std::hash::Hash;
    use std::hash::Hasher;

    pub struct KeyWithDrop {
        pub key: &'static str,
        pub on_drop: Option<Box<dyn FnOnce()>>,
    }

    impl PartialEq for KeyWithDrop {
        fn eq(&self, other: &Self) -> bool {
            self.key == other.key
        }
    }

    impl Eq for KeyWithDrop {}

    impl Hash for KeyWithDrop {
        fn hash<H: Hasher>(&self, state: &mut H) {
            self.key.hash(state);
        }
    }

    impl Borrow<str> for KeyWithDrop {
        fn borrow(&self) -> &str {
            self.key
        }
    }

    impl Drop for KeyWithDrop {
        fn drop(&mut self) {
            if let Some(on_drop) = self.on_drop.take() {
                on_drop();
            }
        }
    }
}
