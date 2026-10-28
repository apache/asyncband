// This file contains code adapted from parking 2.2.1.
// Copyright 2014-2020 The Rust Project Developers
// Asyncband uses the upstream project's Apache-2.0 license option for the incorporated code.
// The incorporated code has been modified for use in Apache Asyncband.
// Upstream source:
// https://github.com/smol-rs/parking/blob/0ece32dbfd6cd1bc1510ede6ed56acb772edf83f/src/lib.rs#L327-L429

use std::cell::Cell;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;
use std::task::Wake;
use std::task::Waker;
use std::thread;
use std::time::Duration;

const EMPTY: usize = 0;
const PARKED: usize = 1;
const NOTIFIED: usize = 2;

pub struct Parker {
    state: Arc<ParkerState>,
    // This marker keeps the single-waiter Parker !Sync while its waker state remains Sync.
    single_waiter: PhantomData<Cell<()>>,
}

struct ParkerState {
    state: AtomicUsize,
    lock: Mutex<()>,
    condvar: Condvar,
}

impl Parker {
    pub fn new() -> Self {
        Self {
            state: Arc::new(ParkerState {
                state: AtomicUsize::new(EMPTY),
                lock: Mutex::new(()),
                condvar: Condvar::new(),
            }),
            single_waiter: PhantomData,
        }
    }

    pub fn park(&self) {
        self.state.park(None);
    }

    pub fn park_timeout(&self, timeout: Duration) {
        self.state.park(Some(timeout));
    }

    pub fn waker(&self) -> Waker {
        Waker::from(self.state.clone())
    }
}

#[inline]
fn yield_now() {
    thread::yield_now();
    #[cfg(test)]
    tests::observe_yield();
}

impl ParkerState {
    fn park(&self, timeout: Option<Duration>) {
        // Notifications are tokens. Consuming one is the common self-wake path and needs no lock.
        if self
            .state
            .compare_exchange(NOTIFIED, EMPTY, SeqCst, SeqCst)
            .is_ok()
        {
            return;
        }

        if timeout == Some(Duration::ZERO) {
            return;
        }

        // Yielding once lets publishers on other threads run before this waiter enters a condvar
        // sleep/wake cycle. Timed waits skip it because their duration was computed before parking,
        // so time spent yielding would extend the caller's deadline.
        if timeout.is_none() {
            yield_now();
            if self
                .state
                .compare_exchange(NOTIFIED, EMPTY, SeqCst, SeqCst)
                .is_ok()
            {
                return;
            }
        }

        // The mutex only closes the race between publishing PARKED and entering the condvar wait.
        let mut guard = self.lock.lock().unwrap();
        match self.state.compare_exchange(EMPTY, PARKED, SeqCst, SeqCst) {
            Ok(_) => {}
            Err(NOTIFIED) => {
                let previous = self.state.swap(EMPTY, SeqCst);
                assert_eq!(previous, NOTIFIED, "park state changed unexpectedly");
                return;
            }
            Err(state) => panic!("inconsistent park state: {state}"),
        }

        match timeout {
            None => loop {
                guard = self.condvar.wait(guard).unwrap();
                if self
                    .state
                    .compare_exchange(NOTIFIED, EMPTY, SeqCst, SeqCst)
                    .is_ok()
                {
                    return;
                }
            },
            Some(timeout) => {
                let (_guard, _wait_result) = self.condvar.wait_timeout(guard, timeout).unwrap();
                match self.state.swap(EMPTY, SeqCst) {
                    NOTIFIED | PARKED => {}
                    state => panic!("inconsistent timed park state: {state}"),
                }
            }
        }
    }

    fn unpark(&self) {
        match self.state.swap(NOTIFIED, SeqCst) {
            EMPTY | NOTIFIED => return,
            PARKED => {}
            state => panic!("inconsistent unpark state: {state}"),
        }

        // Waiting for this lock ensures the parker has entered Condvar::wait before notification.
        drop(self.lock.lock().unwrap());
        self.condvar.notify_one();
    }
}

impl Wake for ParkerState {
    #[inline]
    fn wake(self: Arc<Self>) {
        self.unpark();
    }

    #[inline]
    fn wake_by_ref(self: &Arc<Self>) {
        self.unpark();
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::sync::mpsc;

    use super::*;

    thread_local! {
        static YIELD_COUNT: Cell<usize> = const { Cell::new(0) };
        static WAKE_AFTER_YIELD: RefCell<Option<Waker>> = const { RefCell::new(None) };
    }

    pub(super) fn observe_yield() {
        YIELD_COUNT.with(|count| count.set(count.get() + 1));
        if let Some(waker) = WAKE_AFTER_YIELD.with(|waker| waker.borrow_mut().take()) {
            waker.wake();
        }
    }

    fn reset_yields() {
        YIELD_COUNT.with(|count| count.set(0));
        WAKE_AFTER_YIELD.with(|waker| waker.borrow_mut().take());
    }

    fn yield_count() -> usize {
        YIELD_COUNT.with(Cell::get)
    }

    fn with_watchdog(test: impl FnOnce() + Send + 'static) {
        let (done_tx, done_rx) = mpsc::channel();
        let worker = thread::spawn(move || {
            test();
            let _ = done_tx.send(());
        });
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("blocking test did not complete");
        worker.join().unwrap();
    }

    #[test]
    fn token_park_does_not_yield() {
        reset_yields();
        let parker = Parker::new();
        parker.waker().wake();
        parker.park();
        assert_eq!(yield_count(), 0);
    }

    #[test]
    fn timed_park_does_not_yield() {
        reset_yields();
        let parker = Parker::new();
        parker.park_timeout(Duration::from_millis(1));
        assert_eq!(yield_count(), 0);
    }

    #[test]
    fn notification_during_yield_wakes_park() {
        with_watchdog(|| {
            reset_yields();
            let parker = Parker::new();
            WAKE_AFTER_YIELD.with(|waker| *waker.borrow_mut() = Some(parker.waker()));
            parker.park();
            assert_eq!(yield_count(), 1);
            assert_eq!(parker.state.state.load(SeqCst), EMPTY);
        });
    }

    #[test]
    fn park_sleeps_after_yield_until_woken() {
        with_watchdog(|| {
            reset_yields();
            let parker = Parker::new();
            let state = parker.state.clone();
            let notifier = thread::spawn(move || {
                // Waiting for PARKED forces the park through its condvar wait.
                while state.state.load(SeqCst) != PARKED {
                    thread::yield_now();
                }
                state.unpark();
            });
            parker.park();
            assert_eq!(yield_count(), 1);
            assert_eq!(parker.state.state.load(SeqCst), EMPTY);
            notifier.join().unwrap();
        });
    }
}
