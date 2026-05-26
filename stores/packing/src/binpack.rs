//! First-fit pack assembly, ported from `dev/s3d/sia/upload.go:82-110`
//! (`tryAdd`). The packer is fed pending blobs in arrival order
//! (FIFO from staging) so temporal locality is preserved: blobs
//! written together end up in the same pack.

use crate::manifest::HASH_PREFIX_LEN;

/// A staged entry waiting to be packed.
#[derive(Clone, Debug)]
pub struct PendingBlob {
    /// 12-byte `BLAKE3(path)` prefix — the in-pack index key. Pack
    /// members are sorted ascending by this key for determinism;
    /// since `key` is a prefix of a uniform hash, sorting by `key` is
    /// equivalent to sorting by the full BLAKE3 (collisions are
    /// deduplicated upstream so there are never ties).
    pub key: [u8; HASH_PREFIX_LEN],
    /// The path the caller used in `Store::put_bytes(path, _)`. Used
    /// to look up bytes from staging and to delete after flush.
    pub staging_path: String,
    pub length: u32,
}

/// A pack assembled in memory, ready to be flushed.
#[derive(Clone, Debug, Default)]
pub struct PackGroup {
    pub members: Vec<PendingBlob>,
    pub total_size: u64,
}

impl PackGroup {
    pub fn new() -> Self {
        Self::default()
    }

    /// Current "wasted" tail fraction within the slab grid used for
    /// EC-erasure pricing. Slabs are sized at `slab_size` bytes; a
    /// partial last slab counts toward waste.
    pub fn waste_fraction(&self, slab_size: u64) -> f64 {
        if self.total_size == 0 {
            return 0.0;
        }
        let slabs = self.total_size.div_ceil(slab_size);
        let allocated = slabs * slab_size;
        let waste = allocated - self.total_size;
        waste as f64 / allocated as f64
    }
}

/// Try to add `blob` to `group`. Returns true if accepted.
///
/// Behaviour:
/// - **Empty group.** Accepts unconditionally — every blob has to
///   land somewhere, and a single blob larger than `max_group_size`
///   becomes its own oversized pack rather than getting dropped.
/// - **Loose mode.** While the group's slab-waste fraction is at or
///   above `waste_pct`, any blob is accepted as long as it doesn't
///   push the group past `max_group_size`.
/// - **Tight mode.** Once the waste fraction drops below `waste_pct`,
///   only accept a blob if it fits the remaining slab tail OR
///   strictly reduces the waste fraction.
/// - **Hard cap.** Never grow past `max_group_size` unless the blob
///   exactly fits the remaining slab-aligned tail.
pub fn try_add(
    group: &mut PackGroup,
    blob: &PendingBlob,
    slab_size: u64,
    max_group_size: u64,
    waste_pct: f64,
) -> bool {
    let new_total = group.total_size.saturating_add(blob.length as u64);

    // Empty group: take the blob no matter what. Otherwise a single
    // blob with `length > max_group_size` has nowhere to go and would
    // be silently dropped from `first_fit`'s fresh-group branch.
    if group.members.is_empty() {
        group.members.push(blob.clone());
        group.total_size = new_total;
        return true;
    }

    let current_waste = group.waste_fraction(slab_size);

    let exceeds_max = new_total > max_group_size;
    let slab_remaining = {
        let used = group.total_size % slab_size;
        if used == 0 {
            slab_size
        } else {
            slab_size - used
        }
    };

    let fits_last_slab = (blob.length as u64) <= slab_remaining;
    let in_tight_mode = current_waste < waste_pct;

    if exceeds_max && !fits_last_slab {
        return false;
    }

    if in_tight_mode || exceeds_max {
        // Tight mode: only accept if it reduces waste or fits the tail.
        let mut tentative = group.clone();
        tentative.members.push(blob.clone());
        tentative.total_size = new_total;
        let new_waste = tentative.waste_fraction(slab_size);
        if !fits_last_slab && new_waste >= current_waste {
            return false;
        }
    }

    group.members.push(blob.clone());
    group.total_size = new_total;
    true
}

/// First-fit across `open_groups`. Creates a new group if no existing
/// group accepts the blob. Returns the index of the group the blob
/// landed in.
pub fn first_fit(
    open_groups: &mut Vec<PackGroup>,
    blob: PendingBlob,
    slab_size: u64,
    max_group_size: u64,
    waste_pct: f64,
) -> usize {
    for (i, g) in open_groups.iter_mut().enumerate() {
        if try_add(g, &blob, slab_size, max_group_size, waste_pct) {
            return i;
        }
    }
    let mut g = PackGroup::new();
    let added = try_add(&mut g, &blob, slab_size, max_group_size, waste_pct);
    debug_assert!(added, "fresh group must accept any single blob");
    open_groups.push(g);
    open_groups.len() - 1
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(n: usize, len: u32) -> PendingBlob {
        let path = format!("blob3/{n}");
        let full = *blake3::hash(path.as_bytes()).as_bytes();
        let mut key = [0u8; HASH_PREFIX_LEN];
        key.copy_from_slice(&full[..HASH_PREFIX_LEN]);
        PendingBlob {
            key,
            staging_path: path,
            length: len,
        }
    }

    #[test]
    fn try_add_below_waste_threshold_accepts_freely() {
        let mut g = PackGroup::new();
        assert!(try_add(
            &mut g,
            &b(0, 1_000_000),
            4_000_000,
            256_000_000,
            0.10
        ));
        assert!(try_add(
            &mut g,
            &b(1, 2_000_000),
            4_000_000,
            256_000_000,
            0.10
        ));
        assert_eq!(g.members.len(), 2);
        assert_eq!(g.total_size, 3_000_000);
    }

    #[test]
    fn try_add_rejects_when_would_exceed_max_and_not_fit_tail() {
        let mut g = PackGroup::new();
        g.total_size = 250_000_000;
        g.members.push(b(0, 250_000_000));
        // A 10 MB blob would push us to 260 MB > 256 MB cap and tail
        // remaining is 4 MB - 250 MB % 4 MB = 2 MB so it doesn't fit
        // the tail either → reject.
        let rejected = !try_add(&mut g, &b(1, 10_000_000), 4_000_000, 256_000_000, 0.10);
        assert!(rejected);
    }

    #[test]
    fn first_fit_starts_new_group_when_no_fit() {
        let mut groups = Vec::new();
        first_fit(&mut groups, b(0, 200_000_000), 4_000_000, 256_000_000, 0.10);
        first_fit(&mut groups, b(1, 200_000_000), 4_000_000, 256_000_000, 0.10);
        assert_eq!(groups.len(), 2);
    }

    /// A blob larger than `max_group_size` must still land in a
    /// (necessarily oversized) pack of its own rather than being
    /// silently dropped by `first_fit`'s fresh-group branch.
    #[test]
    fn oversized_blob_gets_its_own_pack() {
        let mut groups = Vec::new();
        let oversized = 300_000_000u32;
        first_fit(&mut groups, b(0, oversized), 4_000_000, 256_000_000, 0.10);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].members.len(), 1);
        assert_eq!(groups[0].total_size, oversized as u64);
    }
}
