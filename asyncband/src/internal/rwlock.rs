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

use std::sync::PoisonError;

pub(crate) struct RwLock<T: ?Sized>(std::sync::RwLock<T>);

impl<T> RwLock<T> {
    pub(crate) const fn new(t: T) -> Self {
        Self(std::sync::RwLock::new(t))
    }
}

impl<T: ?Sized> RwLock<T> {
    pub(crate) fn read(&self) -> std::sync::RwLockReadGuard<'_, T> {
        self.0.read().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn write(&self) -> std::sync::RwLockWriteGuard<'_, T> {
        self.0.write().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::internal::RwLock;

    #[test]
    fn test_poison_rwlock() {
        let rwlock = Arc::new(RwLock::new(42));
        let r = rwlock.clone();
        let handle = std::thread::spawn(move || {
            let _guard = r.write();
            panic!("poison");
        });
        let _ = handle.join();
        assert_eq!(*rwlock.read(), 42);
        assert_eq!(*rwlock.write(), 42);
    }
}
