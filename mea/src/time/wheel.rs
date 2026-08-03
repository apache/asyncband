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

//! Private hierarchical timing wheel used by the explicit timer driver.
//!
//! Finite deadlines are stored in six levels of intrusive slot lists. Deadlines outside the moving
//! wheel horizon stay in an ordered overflow map until they can be promoted; elapsed and
//! unrepresentable deadlines use separate lists.

use std::array;
use std::collections::BTreeMap;
use std::time::Duration;
use std::time::Instant;

use slab::Slab;

const LEVELS: usize = 6;
const SLOTS: usize = u64::BITS as usize;
const TICK_MILLIS: u64 = 1;
const TICK: Duration = Duration::from_millis(TICK_MILLIS);
pub(super) const HORIZON: u64 = (SLOTS as u64).pow(LEVELS as u32);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Deadline {
    At(Instant),
    Never,
}

impl Deadline {
    pub(super) fn checked_add(base: Instant, duration: Duration) -> Self {
        base.checked_add(duration).map_or(Self::Never, Self::At)
    }

    pub(super) fn as_instant(self) -> Option<Instant> {
        match self {
            Self::At(instant) => Some(instant),
            Self::Never => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct List {
    head: Option<usize>,
    tail: Option<usize>,
}

/// The collection currently owning a wheel entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Location {
    Immediate,
    Wheel { level: usize, slot: usize },
    Overflow(Instant),
    Never,
}

struct Node<T> {
    deadline: Deadline,
    location: Option<Location>,
    next: Option<usize>,
    prev: Option<usize>,
    value: T,
}

/// One unit of work charged against the driver's timer-entry budget.
pub(super) enum Step {
    /// One entry was moved between wheel tiers without becoming ready.
    Examined,
    /// The identified entry is ready for the driver to remove and fire.
    Fire(usize),
}

/// Slab-backed timer entries partitioned by deadline range.
pub(super) struct Wheel<T> {
    entries: Slab<Node<T>>,
    genesis: Instant,
    immediate: List,
    never: List,
    // Bit `slot` is set when that level's intrusive slot list is non-empty.
    occupancy: [u64; LEVELS],
    overflow: BTreeMap<Instant, List>,
    position: u64,
    slots: [[List; SLOTS]; LEVELS],
}

impl<T> Wheel<T> {
    pub(super) fn new(genesis: Instant) -> Self {
        Self {
            entries: Slab::new(),
            genesis,
            immediate: List::default(),
            never: List::default(),
            occupancy: [0; LEVELS],
            overflow: BTreeMap::new(),
            position: 0,
            slots: array::from_fn(|_| array::from_fn(|_| List::default())),
        }
    }

    pub(super) fn insert(&mut self, deadline: Deadline, now: Instant, value: T) -> usize {
        let location = self.classify(deadline, now);
        let id = self.entries.insert(Node {
            deadline,
            location: None,
            next: None,
            prev: None,
            value,
        });
        self.link_back(id, location);
        id
    }

    pub(super) fn get(&self, id: usize) -> Option<&T> {
        self.entries.get(id).map(|node| &node.value)
    }

    pub(super) fn remove(&mut self, id: usize) -> Option<T> {
        if !self.entries.contains(id) {
            return None;
        }
        self.unlink(id);
        Some(self.entries.remove(id).value)
    }

    pub(super) fn step_immediate(&self) -> Option<Step> {
        self.immediate.head.map(Step::Fire)
    }

    #[cfg(test)]
    fn step(&mut self, now: Instant) -> Option<Step> {
        self.step_immediate()
            .or_else(|| self.step_non_immediate(now))
    }

    pub(super) fn step_non_immediate(&mut self, now: Instant) -> Option<Step> {
        let now_tick = self.floor_tick(now);
        if let Some((boundary, level, slot)) = self.next_wheel_boundary() {
            if boundary <= now_tick {
                self.position = boundary;
                let location = Location::Wheel { level, slot };
                let id = self
                    .list(location)
                    .head
                    .expect("occupied slot must be non-empty");
                if level == 0 || self.deadline_elapsed(id, now) {
                    return Some(Step::Fire(id));
                }

                self.unlink(id);
                let location = self.classify(self.entries[id].deadline, now);
                self.link_back(id, location);
                return Some(Step::Examined);
            }
        }

        if let Some((&deadline, list)) = self.overflow.first_key_value() {
            if self.overflow_ready(deadline, now) {
                let id = list.head.expect("overflow bucket must be non-empty");
                if deadline <= now {
                    return Some(Step::Fire(id));
                }

                self.position = now_tick;
                self.unlink(id);
                let location = self.classify(self.entries[id].deadline, now);
                self.link_back(id, location);
                return Some(Step::Examined);
            }
        }

        self.position = now_tick;
        None
    }

    pub(super) fn has_due(&self, now: Instant) -> bool {
        if !self.immediate_is_empty() {
            return true;
        }
        let now_tick = self.floor_tick(now);
        if self
            .next_wheel_boundary()
            .is_some_and(|(boundary, _, _)| boundary <= now_tick)
        {
            return true;
        }
        self.overflow
            .first_key_value()
            .is_some_and(|(&deadline, _)| self.overflow_ready(deadline, now))
    }

    pub(super) fn settle(&mut self, now: Instant) {
        debug_assert!(!self.has_due(now));
        self.position = self.floor_tick(now);
    }

    pub(super) fn next_poll_at(&self, now: Instant) -> Option<Instant> {
        if self.has_due(now) {
            return Some(now);
        }

        let wheel = self
            .next_wheel_boundary()
            .and_then(|(tick, _, _)| self.instant_for_tick(tick));
        let overflow = self
            .overflow
            .first_key_value()
            .map(|(&deadline, _)| self.promotion_instant(deadline));
        match (wheel, overflow) {
            (Some(wheel), Some(overflow)) => Some(wheel.min(overflow)),
            (Some(wheel), None) => Some(wheel),
            (None, Some(overflow)) => Some(overflow),
            (None, None) => None,
        }
    }

    pub(super) fn drain(&mut self) -> impl Iterator<Item = T> + '_ {
        self.immediate = List::default();
        self.never = List::default();
        self.occupancy = [0; LEVELS];
        self.overflow.clear();
        self.slots = array::from_fn(|_| array::from_fn(|_| List::default()));
        self.entries.drain().map(|node| node.value)
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    fn classify(&self, deadline: Deadline, now: Instant) -> Location {
        let Deadline::At(deadline) = deadline else {
            return Location::Never;
        };
        if deadline <= now {
            return Location::Immediate;
        }

        let Some(tick) = self.ceil_tick(deadline) else {
            return Location::Overflow(deadline);
        };
        let Some(delta) = tick.checked_sub(self.position) else {
            return Location::Immediate;
        };
        if delta >= HORIZON {
            return Location::Overflow(deadline);
        }

        let level = level_for_delta(delta);
        let width = level_width(level);
        let slot = ((tick / width) % SLOTS as u64) as usize;
        Location::Wheel { level, slot }
    }

    fn deadline_elapsed(&self, id: usize, now: Instant) -> bool {
        match self.entries[id].deadline {
            Deadline::At(deadline) => deadline <= now,
            Deadline::Never => false,
        }
    }

    fn floor_tick(&self, instant: Instant) -> u64 {
        if instant <= self.genesis {
            return 0;
        }
        let elapsed_millis = instant.duration_since(self.genesis).as_millis();
        let ticks = elapsed_millis / u128::from(TICK_MILLIS);
        u64::try_from(ticks).unwrap_or(u64::MAX)
    }

    fn ceil_tick(&self, instant: Instant) -> Option<u64> {
        if instant <= self.genesis {
            return Some(0);
        }
        let nanos = instant.duration_since(self.genesis).as_nanos();
        let tick_nanos = TICK.as_nanos();
        let ticks = nanos.checked_add(tick_nanos - 1)? / tick_nanos;
        u64::try_from(ticks).ok()
    }

    fn instant_for_tick(&self, tick: u64) -> Option<Instant> {
        tick.checked_mul(TICK_MILLIS)
            .and_then(|millis| self.genesis.checked_add(Duration::from_millis(millis)))
    }

    fn promotion_instant(&self, deadline: Instant) -> Instant {
        let Some(tick) = self.ceil_tick(deadline) else {
            // This deadline is outside the wheel's u64 tick domain. Keep it in overflow until the
            // deadline itself rather than repeatedly attempting an impossible promotion.
            return deadline;
        };
        tick.checked_sub(HORIZON - 1)
            .and_then(|tick| self.instant_for_tick(tick))
            .unwrap_or(self.genesis)
    }

    fn overflow_ready(&self, deadline: Instant, now: Instant) -> bool {
        self.promotion_instant(deadline) <= now
    }

    fn immediate_is_empty(&self) -> bool {
        self.immediate.head.is_none()
    }

    fn next_wheel_boundary(&self) -> Option<(u64, usize, usize)> {
        let mut result = None;
        for (level, &occupancy) in self.occupancy.iter().enumerate() {
            let mut bits = occupancy;
            while bits != 0 {
                let slot = bits.trailing_zeros() as usize;
                bits &= bits - 1;
                let Some(boundary) = occurrence_start(self.position, level, slot) else {
                    debug_assert!(
                        false,
                        "occupied slot {slot} at level {level} overflowed the tick domain"
                    );
                    continue;
                };
                if result.is_none_or(|(current, _, _)| boundary < current) {
                    result = Some((boundary, level, slot));
                }
            }
        }
        result
    }

    fn list(&self, location: Location) -> List {
        match location {
            Location::Immediate => self.immediate,
            Location::Wheel { level, slot } => self.slots[level][slot],
            Location::Overflow(deadline) => self.overflow[&deadline],
            Location::Never => self.never,
        }
    }

    fn set_list(&mut self, location: Location, list: List) {
        match location {
            Location::Immediate => self.immediate = list,
            Location::Wheel { level, slot } => {
                self.slots[level][slot] = list;
                if list.head.is_some() {
                    self.occupancy[level] |= 1 << slot;
                } else {
                    self.occupancy[level] &= !(1 << slot);
                }
            }
            Location::Overflow(deadline) => {
                if list.head.is_some() {
                    self.overflow.insert(deadline, list);
                } else {
                    self.overflow.remove(&deadline);
                }
            }
            Location::Never => self.never = list,
        }
    }

    fn link_back(&mut self, id: usize, location: Location) {
        debug_assert!(self.entries[id].location.is_none());
        let mut list = match location {
            Location::Overflow(deadline) => {
                self.overflow.get(&deadline).copied().unwrap_or_default()
            }
            _ => self.list(location),
        };
        if let Some(tail) = list.tail {
            self.entries[tail].next = Some(id);
        } else {
            list.head = Some(id);
        }
        self.entries[id].location = Some(location);
        self.entries[id].prev = list.tail;
        self.entries[id].next = None;
        list.tail = Some(id);
        self.set_list(location, list);
    }

    fn unlink(&mut self, id: usize) {
        let node = &self.entries[id];
        let location = node.location.expect("linked node must have a location");
        let prev = node.prev;
        let next = node.next;
        let mut list = self.list(location);

        if let Some(prev) = prev {
            self.entries[prev].next = next;
        } else {
            list.head = next;
        }
        if let Some(next) = next {
            self.entries[next].prev = prev;
        } else {
            list.tail = prev;
        }

        let node = &mut self.entries[id];
        node.location = None;
        node.prev = None;
        node.next = None;
        self.set_list(location, list);
    }
}

fn level_width(level: usize) -> u64 {
    (SLOTS as u64).pow(level as u32)
}

fn level_for_delta(delta: u64) -> usize {
    for level in 0..LEVELS - 1 {
        if delta < level_width(level + 1) {
            return level;
        }
    }
    LEVELS - 1
}

fn occurrence_start(position: u64, level: usize, slot: usize) -> Option<u64> {
    let width = level_width(level);
    let cycle = width.checked_mul(SLOTS as u64)?;
    let base = position / cycle * cycle;
    let mut candidate = base.checked_add((slot as u64).checked_mul(width)?)?;
    if candidate < position {
        candidate = candidate.checked_add(cycle)?;
    }
    Some(candidate)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn at(genesis: Instant, millis: u64) -> Instant {
        genesis
            .checked_add(Duration::from_millis(millis))
            .expect("test instant must be representable")
    }

    fn reset_randomized_wheel(
        wheel: &mut Wheel<u64>,
        oracle: &mut BTreeMap<u64, u64>,
        live: &mut Vec<(usize, u64)>,
        genesis: Instant,
    ) {
        for (wheel_id, oracle_id) in live.drain(..) {
            assert_eq!(wheel.remove(wheel_id), Some(oracle_id));
            assert!(oracle.remove(&oracle_id).is_some());
        }
        assert!(oracle.is_empty());
        *wheel = Wheel::new(genesis);
    }

    #[test]
    fn levels_cover_exact_horizon() {
        assert_eq!(level_for_delta(63), 0);
        assert_eq!(level_for_delta(64), 1);
        assert_eq!(level_for_delta(4095), 1);
        assert_eq!(level_for_delta(4096), 2);
        assert_eq!(level_for_delta(HORIZON - 1), 5);
    }

    #[test]
    fn cascades_at_range_start_and_never_fires_early() {
        let genesis = Instant::now();
        let mut wheel = Wheel::new(genesis);
        let id = wheel.insert(Deadline::At(at(genesis, 65)), genesis, 7);

        assert!(wheel.step(at(genesis, 63)).is_none());
        assert!(matches!(wheel.step(at(genesis, 64)), Some(Step::Examined)));
        assert!(wheel.get(id).is_some());
        assert!(wheel.step(at(genesis, 64)).is_none());
        assert!(matches!(wheel.step(at(genesis, 65)), Some(Step::Fire(found)) if found == id));
        assert_eq!(wheel.remove(id), Some(7));
    }

    #[test]
    fn promotes_at_exact_horizon_boundary() {
        let genesis = Instant::now();
        let mut wheel = Wheel::new(genesis);
        let deadline = at(genesis, HORIZON);
        let id = wheel.insert(Deadline::At(deadline), genesis, 9);

        assert_eq!(wheel.next_poll_at(genesis), Some(at(genesis, 1)));
        assert!(matches!(wheel.step(at(genesis, 1)), Some(Step::Examined)));
        assert!(wheel.get(id).is_some());
        assert!(wheel.step(at(genesis, HORIZON - 1)).is_none());
        assert!(matches!(wheel.step(deadline), Some(Step::Fire(found)) if found == id));
    }

    #[test]
    fn submillisecond_overflow_waits_for_representable_position() {
        let genesis = Instant::now();
        let mut wheel = Wheel::new(genesis);
        let deadline = genesis + Duration::from_micros(HORIZON * 1_000 + 500);
        let id = wheel.insert(Deadline::At(deadline), genesis, 11);

        assert_eq!(wheel.next_poll_at(genesis), Some(at(genesis, 2)));
        assert!(wheel.step(genesis + Duration::from_micros(1_500)).is_none());
        assert!(matches!(wheel.step(at(genesis, 2)), Some(Step::Examined)));
        assert!(wheel.get(id).is_some());
    }

    #[test]
    fn cancellation_unlinks_every_location() {
        let genesis = Instant::now();
        let mut wheel = Wheel::new(genesis);
        let immediate = wheel.insert(Deadline::At(genesis), genesis, 1);
        let near = wheel.insert(Deadline::At(at(genesis, 1)), genesis, 2);
        let overflow = wheel.insert(Deadline::At(at(genesis, HORIZON)), genesis, 3);
        let never = wheel.insert(Deadline::Never, genesis, 4);

        assert_eq!(wheel.remove(immediate), Some(1));
        assert_eq!(wheel.remove(near), Some(2));
        assert_eq!(wheel.remove(overflow), Some(3));
        assert_eq!(wheel.remove(never), Some(4));
        assert_eq!(wheel.len(), 0);
        assert_eq!(wheel.next_poll_at(genesis), None);
    }

    #[test]
    fn randomized_operations_match_ordered_oracle() {
        const MAX_TEST_MILLIS: u64 = HORIZON * 8;

        let genesis = Instant::now();
        let mut wheel = Wheel::new(genesis);
        let mut oracle = BTreeMap::<u64, u64>::new();
        let mut live = Vec::new();
        let mut now = 0_u64;
        let mut next_id = 0_u64;
        let mut random = 0x4d59_5df4_d0f3_3173_u64;

        for _ in 0..5_000 {
            random = random
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            match random % 5 {
                0 if !live.is_empty() => {
                    let index = (random as usize / 5) % live.len();
                    let (wheel_id, oracle_id) = live.swap_remove(index);
                    assert_eq!(wheel.remove(wheel_id), Some(oracle_id));
                    oracle.remove(&oracle_id);
                }
                1 | 2 => {
                    let jump = match random % 8 {
                        0 => 1,
                        1 => 63,
                        2 => 64,
                        3 => 65,
                        4 => 4096,
                        5 => HORIZON - 1,
                        6 => HORIZON,
                        _ => random % 10_000,
                    };
                    let candidate = now.checked_add(jump).unwrap();
                    if candidate > MAX_TEST_MILLIS {
                        reset_randomized_wheel(&mut wheel, &mut oracle, &mut live, genesis);
                        now = jump;
                    } else {
                        now = candidate;
                    }
                    while let Some(step) = wheel.step(at(genesis, now)) {
                        if let Step::Fire(id) = step {
                            let value = wheel.remove(id).unwrap();
                            let deadline = oracle.remove(&value).unwrap();
                            assert!(deadline <= now, "timer {value} fired early");
                            if let Some(index) =
                                live.iter().position(|&(wheel_id, _)| wheel_id == id)
                            {
                                live.swap_remove(index);
                            }
                        }
                    }
                    let expected: Vec<_> = oracle
                        .iter()
                        .filter_map(|(&id, &deadline)| (deadline <= now).then_some(id))
                        .collect();
                    assert!(
                        expected.is_empty(),
                        "due timers were not fired: {expected:?}"
                    );
                }
                _ => {
                    let distance = match random % 7 {
                        0 => 0,
                        1 => 1,
                        2 => 63,
                        3 => 64,
                        4 => 4096,
                        5 => HORIZON - 1,
                        _ => HORIZON,
                    };
                    let mut deadline = now.checked_add(distance).unwrap();
                    if deadline > MAX_TEST_MILLIS {
                        reset_randomized_wheel(&mut wheel, &mut oracle, &mut live, genesis);
                        now = 0;
                        deadline = distance;
                    }
                    let id = wheel.insert(
                        Deadline::At(at(genesis, deadline)),
                        at(genesis, now),
                        next_id,
                    );
                    oracle.insert(next_id, deadline);
                    live.push((id, next_id));
                    next_id += 1;
                }
            }
        }
        reset_randomized_wheel(&mut wheel, &mut oracle, &mut live, genesis);
    }
}
