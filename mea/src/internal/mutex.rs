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

use std::fmt;
use std::sync::PoisonError;

pub(crate) struct Mutex<T: ?Sized>(std::sync::Mutex<T>);

impl<T: ?Sized + fmt::Debug> fmt::Debug for Mutex<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl<T> Mutex<T> {
    pub(crate) const fn new(t: T) -> Self {
        Self(std::sync::Mutex::new(t))
    }

    pub(crate) fn lock(&self) -> std::sync::MutexGuard<'_, T> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::internal::Mutex;

    #[test]
    fn test_poison_mutex() {
        let mutex = Arc::new(Mutex::new(42));
        let m = mutex.clone();
        let handle = std::thread::spawn(move || {
            let _guard = m.lock();
            panic!("poison");
        });
        let _ = handle.join();
        let guard = mutex.lock();
        assert_eq!(*guard, 42);
    }
}
