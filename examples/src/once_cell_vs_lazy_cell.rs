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

use std::future::Ready;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use asyncband::once::LazyCell;
use asyncband::once::OnceCell;
use tokio::sync::Notify;

static ONCE_ENDPOINT: OnceCell<String> = OnceCell::new();
static LAZY_ENDPOINT: LazyCell<String, Ready<String>> = LazyCell::new(load_default_endpoint);

fn load_default_endpoint() -> Ready<String> {
    std::future::ready("https://service.example".to_owned())
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    fixed_static_initializer_can_use_either_cell().await;
    once_cell_accepts_access_time_context().await;
    lazy_cell_owns_a_local_fn_once().await;
}

async fn fixed_static_initializer_can_use_either_cell() {
    // When restarting after cancellation is acceptable, a stateless, repeatable initializer does
    // not require `LazyCell`. An accessor can pass the same free function to `OnceCell`.
    assert_eq!(
        ONCE_ENDPOINT.get_or_init(load_default_endpoint).await,
        "https://service.example"
    );

    // `LazyCell` instead stores that fixed initializer in the static itself, so callers only need
    // to force the value.
    assert_eq!(
        LazyCell::force(&LAZY_ENDPOINT).await,
        "https://service.example"
    );
}

async fn once_cell_accepts_access_time_context() {
    let endpoint = OnceCell::new();

    // Failed attempts leave `OnceCell` empty. A later caller can retry with fresh context.
    let first = endpoint
        .get_or_try_init(async || Err::<String, _>("service discovery unavailable"))
        .await;
    assert_eq!(first.unwrap_err(), "service discovery unavailable");

    let discovered_endpoint = "https://discovered.example".to_owned();
    let endpoint = endpoint
        .get_or_try_init(async move || Ok::<_, &'static str>(discovered_endpoint))
        .await
        .unwrap();
    assert_eq!(endpoint, "https://discovered.example");
}

async fn lazy_cell_owns_a_local_fn_once() {
    struct Credentials {
        token: String,
    }

    struct Client {
        token: String,
    }

    let attempts = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let resume = Arc::new(Notify::new());
    let credentials = Credentials {
        token: "secret".to_owned(),
    };

    let initialize = {
        let attempts = Arc::clone(&attempts);
        let started = Arc::clone(&started);
        let resume = Arc::clone(&resume);

        // Moving `credentials.token` out makes this local initializer `FnOnce`, not `Fn`.
        move || {
            let token = credentials.token;

            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                started.notify_one();
                resume.notified().await;
                Client { token }
            }
        }
    };

    // A caller could pass `initialize` to `OnceCell::get_or_init` once, but the call consumes it.
    // If that call is cancelled, a later caller cannot supply the same `FnOnce` again. `LazyCell`
    // owns the initializer and preserves its in-flight future across callers.
    let client = Arc::pin(LazyCell::new(initialize));

    let first_caller = tokio::spawn({
        let client = client.clone();
        async move {
            LazyCell::force_pin(client.as_ref()).await;
        }
    });
    started.notified().await;
    first_caller.abort();
    assert!(first_caller.await.unwrap_err().is_cancelled());

    // Cancellation does not consume the captured credentials or restart the initializer. The next
    // caller resumes the same future.
    resume.notify_one();
    assert_eq!(LazyCell::force_pin(client.as_ref()).await.token, "secret");
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}
