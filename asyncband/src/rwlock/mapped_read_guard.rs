// Portions derived from Tokio 1.42.0's RwLock implementation.
// Copyright (c) Tokio Contributors
// Licensed under the MIT License.
// Modified by the Apache Software Foundation.
// See the project LICENSE file for the exact upstream revision and source paths.

use std::fmt;
use std::marker::PhantomData;
use std::ops::Deref;
use std::ptr::NonNull;

use crate::internal::semaphore;

/// RAII structure used to release the shared read access of a lock when dropped, for a mapped
/// component of the locked data.
///
/// This structure is created by the [`map`] and [`filter_map`] methods on [`RwLockReadGuard`]. It
/// allows you to hold a read lock on a subfield of the protected data, enabling more fine-grained
/// access control while maintaining the same locking semantics.
///
/// As long as you have this guard, you have shared read access to the underlying `T`. The guard
/// internally keeps a reference to the original rwlock's semaphore, so the original lock is
/// maintained until this guard is dropped.
///
/// `MappedRwLockReadGuard` implements [`Send`] and [`Sync`] when `T: Sync`, allowing it to be
/// used across task boundaries and shared between threads safely. Note that [`Send`] does not
/// require `T: Send` because the read guard only borrows the data rather than owning it.
///
/// [`map`]: crate::rwlock::RwLockReadGuard::map
/// [`filter_map`]: crate::rwlock::RwLockReadGuard::filter_map
/// [`RwLockReadGuard`]: crate::rwlock::RwLockReadGuard
///
/// See the [module level documentation](crate::rwlock) for more.
///
/// # Examples
///
/// ```
/// # #[tokio::main]
/// # async fn main() {
/// use asyncband::rwlock::RwLock;
/// use asyncband::rwlock::RwLockReadGuard;
///
/// #[derive(Debug)]
/// struct User {
///     id: u32,
///     profile: UserProfile,
/// }
///
/// #[derive(Debug)]
/// struct UserProfile {
///     email: String,
///     name: String,
/// }
///
/// let user = User {
///     id: 1,
///     profile: UserProfile {
///         email: "user@example.com".to_owned(),
///         name: "Alice".to_owned(),
///     },
/// };
///
/// let rwlock = RwLock::new(user);
/// let guard = rwlock.read().await;
/// let profile_guard = RwLockReadGuard::map(guard, |user| &user.profile);
///
/// // Now we can only access the user's profile
/// assert_eq!(profile_guard.email, "user@example.com");
/// # }
/// ```
#[must_use = "if unused the RwLock will immediately unlock"]
pub struct MappedRwLockReadGuard<'a, T: ?Sized> {
    d: NonNull<T>,
    s: &'a semaphore::Semaphore,
    variance: PhantomData<fn() -> T>,
}

// SAFETY: MappedRwLockReadGuard is Send when T: Sync. We don't require T: Send because
// the guard RwLockReadGuard doesn't transfer ownership of T - it only holds a shared reference.
// When moved to another thread, the guard maintains the read lock and the new thread
// can safely access &T (which is allowed since T: Sync). The semaphore reference
// and NonNull pointer are both safe to transfer between threads.
unsafe impl<T: ?Sized + Sync> Send for MappedRwLockReadGuard<'_, T> {}

// SAFETY: `&MappedRwLockReadGuard` can be shared between threads if `T: Sync`.
// Accessing the guard only provides a `&T`, which is safe to share concurrently when `T: Sync`.
unsafe impl<T: ?Sized + Sync> Sync for MappedRwLockReadGuard<'_, T> {}

impl<'a, T: ?Sized> MappedRwLockReadGuard<'a, T> {
    pub(crate) fn new(d: NonNull<T>, s: &'a semaphore::Semaphore) -> Self {
        Self {
            d,
            s,
            variance: PhantomData,
        }
    }
}

impl<T: ?Sized> Drop for MappedRwLockReadGuard<'_, T> {
    fn drop(&mut self) {
        self.s.release(1);
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for MappedRwLockReadGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: ?Sized + fmt::Display> fmt::Display for MappedRwLockReadGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}

impl<T: ?Sized> Deref for MappedRwLockReadGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        // SAFETY: we hold the read lock and the NonNull pointer is valid for the guard's lifetime
        unsafe { self.d.as_ref() }
    }
}

impl<'a, T: ?Sized> MappedRwLockReadGuard<'a, T> {
    /// Makes a new [`MappedRwLockReadGuard`] for a component of the locked data.
    ///
    /// This operation cannot fail as the `MappedRwLockReadGuard` passed in already locked the
    /// rwlock.
    ///
    /// This is an associated function that needs to be used as `MappedRwLockReadGuard::map(...)`.
    ///
    /// A method would interfere with methods of the same name on the contents of the locked data.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::rwlock::MappedRwLockReadGuard;
    /// use asyncband::rwlock::RwLock;
    /// use asyncband::rwlock::RwLockReadGuard;
    ///
    /// #[derive(Debug)]
    /// struct User {
    ///     id: u32,
    ///     profile: UserProfile,
    /// }
    ///
    /// #[derive(Debug)]
    /// struct UserProfile {
    ///     email: String,
    ///     name: String,
    /// }
    ///
    /// let user = User {
    ///     id: 1,
    ///     profile: UserProfile {
    ///         email: "user@example.com".to_owned(),
    ///         name: "Alice".to_owned(),
    ///     },
    /// };
    ///
    /// let rwlock = RwLock::new(user);
    /// let guard = rwlock.read().await;
    /// // First map to the profile field
    /// let profile_guard = RwLockReadGuard::map(guard, |user| &user.profile);
    /// // Then map to the email field specifically
    /// let email_guard = MappedRwLockReadGuard::map(profile_guard, |profile| &profile.email);
    ///
    /// assert_eq!(&*email_guard, "user@example.com");
    /// # }
    /// ```
    pub fn map<U, F>(orig: Self, f: F) -> MappedRwLockReadGuard<'a, U>
    where
        F: FnOnce(&T) -> &U,
        U: ?Sized,
    {
        // SAFETY: orig.d is a valid NonNull<T> pointer that was created from a valid reference
        // when the original MappedRwLockReadGuard was constructed. The guard guarantees shared
        // access to the data through the rwlock, so dereferencing is safe.
        let d = NonNull::from(f(unsafe { orig.d.as_ref() }));
        let orig = std::mem::ManuallyDrop::new(orig);
        MappedRwLockReadGuard::new(d, orig.s)
    }

    /// Attempts to make a new [`MappedRwLockReadGuard`] for a component of the locked data. The
    /// original guard is returned if the closure returns `None`.
    ///
    /// This operation cannot fail as the `MappedRwLockReadGuard` passed in already locked the
    /// rwlock.
    ///
    /// This is an associated function that needs to be used as
    /// `MappedRwLockReadGuard::filter_map(...)`.
    ///
    /// A method would interfere with methods of the same name on the contents of the locked data.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::rwlock::MappedRwLockReadGuard;
    /// use asyncband::rwlock::RwLock;
    /// use asyncband::rwlock::RwLockReadGuard;
    ///
    /// #[derive(Debug)]
    /// struct Person {
    ///     name: String,
    ///     email: Option<String>,
    /// }
    ///
    /// let person = Person {
    ///     name: "Alice".to_owned(),
    ///     email: Some("alice@example.com".to_owned()),
    /// };
    ///
    /// let rwlock = RwLock::new(person);
    /// let guard = rwlock.read().await;
    /// let name_guard = RwLockReadGuard::map(guard, |person| &person.name);
    ///
    /// // Try to map to the email if it exists
    /// let person_guard = rwlock.read().await;
    /// let email_result = MappedRwLockReadGuard::filter_map(
    ///     RwLockReadGuard::map(person_guard, |person| &person.email),
    ///     |email_opt| email_opt.as_ref(),
    /// );
    ///
    /// match email_result {
    ///     Ok(email_guard) => {
    ///         assert_eq!(&*email_guard, "alice@example.com");
    ///     }
    ///     Err(_original_guard) => {
    ///         // Email was None, original guard is returned
    ///         println!("No email available");
    ///     }
    /// }
    /// # }
    /// ```
    pub fn filter_map<U, F>(orig: Self, f: F) -> Result<MappedRwLockReadGuard<'a, U>, Self>
    where
        F: FnOnce(&T) -> Option<&U>,
        U: ?Sized,
    {
        // SAFETY: orig.d is a valid NonNull<T> pointer that was created from a valid reference
        // when the original MappedRwLockReadGuard was constructed. The guard guarantees shared
        // access to the data through the rwlock, so dereferencing is safe.
        match f(unsafe { orig.d.as_ref() }) {
            Some(d) => {
                let d = NonNull::from(d);
                let orig = std::mem::ManuallyDrop::new(orig);
                Ok(MappedRwLockReadGuard::new(d, orig.s))
            }
            None => Err(orig),
        }
    }
}
