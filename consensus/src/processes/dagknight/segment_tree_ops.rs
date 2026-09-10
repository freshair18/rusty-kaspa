use std::ops::{AddAssign, Range, Sub};

use num_traits::Zero;

type LeafPosition = usize;
type NodeIndex = usize;

/// Node relationships in the tree's one-based heap layout.
pub(super) fn left_child(node: NodeIndex) -> NodeIndex {
    node * 2
}

pub(super) fn right_child(node: NodeIndex) -> NodeIndex {
    node * 2 + 1
}

pub(super) fn parent(node: NodeIndex) -> NodeIndex {
    debug_assert!(node > 1, "root node has no parent");
    node / 2
}

#[repr(u8)]
#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
enum BoundaryKind {
    End = 0,
    Start = 1,
}

#[derive(Clone, Copy)]
struct RangeBoundary<S> {
    position: LeafPosition,
    kind: BoundaryKind,
    delta: S,
}

impl<S> RangeBoundary<S> {
    fn start(position: LeafPosition, delta: S) -> Self {
        Self { position, kind: BoundaryKind::Start, delta }
    }

    fn end(position: LeafPosition, delta: S) -> Self {
        Self { position, kind: BoundaryKind::End, delta }
    }
}

/// Half-open ranges that only touch at a boundary are disjoint.
pub(super) fn ranges_are_disjoint(first: &Range<LeafPosition>, second: &Range<LeafPosition>) -> bool {
    first.end <= second.start || second.end <= first.start
}

/// Returns whether `outer` contains every position in `inner`.
pub(super) fn range_fully_contains(outer: &Range<LeafPosition>, inner: &Range<LeafPosition>) -> bool {
    outer.start <= inner.start && inner.end <= outer.end
}

/// Splits a non-leaf range into its two contiguous child ranges.
pub(super) fn split_range(range: &Range<LeafPosition>) -> (Range<LeafPosition>, Range<LeafPosition>) {
    debug_assert!(range.len() > 1, "cannot split a leaf range");
    let midpoint = range.start + range.len() / 2;
    (range.start..midpoint, midpoint..range.end)
}

pub(super) fn coalesce_ranges<S>(ranges: &[(Range<LeafPosition>, S)]) -> Vec<(Range<LeafPosition>, S)>
where
    S: Copy + PartialOrd + AddAssign + Sub<Output = S> + Zero,
{
    // Represent every range by a start event and an end event. The sweep
    // between two consecutive positions has one constant combined delta.
    let mut boundary_events = Vec::with_capacity(ranges.len() * 2);
    for (range, delta) in ranges {
        if !range.is_empty() && !delta.is_zero() {
            boundary_events.push(RangeBoundary::start(range.start, *delta));
            boundary_events.push(RangeBoundary::end(range.end, *delta));
        }
    }
    // End events sort before start events at the same position, so an ended
    // range is removed before a new range beginning there is added.
    boundary_events.sort_unstable_by_key(|event| (event.position, event.kind));

    let mut coalesced_ranges = Vec::new();
    let mut active_delta = S::zero();
    let mut previous_boundary = None;
    let mut event_index = 0;
    while event_index < boundary_events.len() {
        let current_boundary = boundary_events[event_index].position;
        if let Some(previous_boundary) = previous_boundary
            && previous_boundary < current_boundary
            && !active_delta.is_zero()
        {
            coalesced_ranges.push((previous_boundary..current_boundary, active_delta));
        }

        while event_index < boundary_events.len() && boundary_events[event_index].position == current_boundary {
            let event = boundary_events[event_index];
            active_delta = match event.kind {
                BoundaryKind::Start => active_delta + event.delta,
                BoundaryKind::End => active_delta - event.delta,
            };
            event_index += 1;
        }
        previous_boundary = Some(current_boundary);
    }
    coalesced_ranges
}
