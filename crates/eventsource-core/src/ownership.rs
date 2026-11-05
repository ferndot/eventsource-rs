//! Single-owner work distribution by rendezvous hashing.

use std::collections::BTreeMap;

/// FNV-1a over `subscription`, a separator and `member`, then a 64-bit mixer.
///
/// Hand-rolled so the result is stable across processes, platforms and Rust
/// releases (`DefaultHasher` guarantees none of these).
fn weight(subscription: &str, member: &str) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut h = OFFSET;
    for byte in subscription
        .bytes()
        .chain(std::iter::once(0xff))
        .chain(member.bytes())
    {
        h ^= u64::from(byte);
        h = h.wrapping_mul(PRIME);
    }
    // splitmix64 finalizer: FNV alone mixes the last bytes poorly.
    h ^= h >> 30;
    h = h.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    h ^= h >> 27;
    h = h.wrapping_mul(0x94d0_49bb_1331_11eb);
    h ^ (h >> 31)
}

/// The member that owns `subscription`: the one with the highest rendezvous
/// weight, ties broken by the smaller member id. `None` if there are no members.
///
/// The result does not depend on the order of `members`.
#[must_use]
pub fn owner_of<'m>(subscription: &str, members: &'m [String]) -> Option<&'m str> {
    members
        .iter()
        .max_by(|a, b| {
            weight(subscription, a)
                .cmp(&weight(subscription, b))
                .then_with(|| b.cmp(a))
        })
        .map(String::as_str)
}

/// Assigns each subscription to exactly one of the live `members`.
///
/// Empty when there are no members. Removing a member only reassigns the
/// subscriptions it owned; adding one only moves subscriptions to it.
#[must_use]
pub fn assign(subscriptions: &[String], members: &[String]) -> BTreeMap<String, String> {
    subscriptions
        .iter()
        .filter_map(|sub| owner_of(sub, members).map(|owner| (sub.clone(), owner.to_owned())))
        .collect()
}

/// One member's view of how work is split across the live members.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionOwnership {
    me: String,
    members: Vec<String>,
}

impl SubscriptionOwnership {
    /// `me` is this member's id; `members` are the live members. `me` is not
    /// added implicitly, so a member absent from `members` owns nothing.
    #[must_use]
    pub fn new(me: impl Into<String>, members: impl IntoIterator<Item = String>) -> Self {
        Self {
            me: me.into(),
            members: members.into_iter().collect(),
        }
    }

    /// Whether this member owns `subscription`.
    #[must_use]
    pub fn owns(&self, subscription: &str) -> bool {
        owner_of(subscription, &self.members) == Some(self.me.as_str())
    }

    /// The subset of `subscriptions` this member should run, in input order.
    #[must_use]
    pub fn mine(&self, subscriptions: &[String]) -> Vec<String> {
        subscriptions
            .iter()
            .filter(|sub| self.owns(sub))
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("projection-{i}")).collect()
    }

    fn members(ids: &[&str]) -> Vec<String> {
        ids.iter().map(|s| (*s).to_owned()).collect()
    }

    #[test]
    fn every_subscription_has_exactly_one_live_owner() {
        let live = members(&["m1", "m2", "m3"]);
        let subs = names(100);
        let assignment = assign(&subs, &live);
        assert_eq!(assignment.len(), 100, "one entry per subscription");
        for sub in &subs {
            let owner = assignment.get(sub).expect("every subscription is assigned");
            assert!(live.contains(owner), "{owner} is a live member");
        }
    }

    #[test]
    fn removing_a_member_moves_only_its_subscriptions() {
        let subs = names(100);
        let before = assign(&subs, &members(&["m1", "m2", "m3"]));
        let after = assign(&subs, &members(&["m1", "m3"]));
        for sub in &subs {
            if before[sub] == "m2" {
                assert_ne!(after[sub], "m2");
            } else {
                assert_eq!(before[sub], after[sub], "{sub} must not move");
            }
        }
    }

    #[test]
    fn adding_a_member_moves_only_to_the_new_member_and_few() {
        let subs = names(100);
        let before = assign(&subs, &members(&["m1", "m2", "m3"]));
        let after = assign(&subs, &members(&["m1", "m2", "m3", "m4"]));
        let moved: Vec<_> = subs.iter().filter(|s| before[*s] != after[*s]).collect();
        for sub in &moved {
            assert_eq!(after[*sub], "m4", "{sub} may only move to the new member");
        }
        assert!(moved.len() < 45, "moved {} of 100", moved.len());
    }

    #[test]
    fn assignment_ignores_member_order() {
        let subs = names(100);
        let a = assign(&subs, &members(&["m1", "m2", "m3"]));
        let b = assign(&subs, &members(&["m3", "m1", "m2"]));
        assert_eq!(a, b);
    }

    #[test]
    fn members_partition_the_subscriptions_between_them() {
        let subs = names(100);
        let live = members(&["m1", "m2", "m3"]);
        let mut all: Vec<String> = Vec::new();
        for me in &live {
            let mine = SubscriptionOwnership::new(me.clone(), live.clone()).mine(&subs);
            assert!(!mine.is_empty(), "{me} gets some work");
            all.extend(mine);
        }
        all.sort();
        let mut expected = subs.clone();
        expected.sort();
        assert_eq!(
            all, expected,
            "each subscription runs on exactly one member"
        );
        assert!(
            SubscriptionOwnership::new("m9", live.clone())
                .mine(&subs)
                .is_empty()
        );
    }
}
