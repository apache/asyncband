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

use std::any::type_name;
use std::fmt;

/// An error returned when a value cannot complete a [`Completion`](super::Completion).
///
/// Completion is rejected after another value has already won or when no observers remain. The
/// rejected value can be retrieved with [`CompleteError::into_inner`].
#[derive(Clone, PartialEq, Eq)]
pub struct CompleteError<T>(T);

impl<T> CompleteError<T> {
    /// Returns a reference to the value that could not complete the primitive.
    pub fn as_inner(&self) -> &T {
        &self.0
    }

    /// Consumes the error and returns the value that could not complete the primitive.
    pub fn into_inner(self) -> T {
        self.0
    }

    pub(super) fn new(value: T) -> Self {
        Self(value)
    }
}

impl<T> fmt::Display for CompleteError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("completion rejected")
    }
}

impl<T> fmt::Debug for CompleteError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CompleteError<{}>(..)", type_name::<T>())
    }
}

impl<T> std::error::Error for CompleteError<T> {}

/// An error returned when the completer is dropped before providing a value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitError {
    /// The completer was dropped before providing a value.
    Closed,
}

impl fmt::Display for WaitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("completion closed before a value was provided")
    }
}

impl std::error::Error for WaitError {}
