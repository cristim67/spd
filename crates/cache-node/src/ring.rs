//! Consistent hashing cu noduri virtuale.
//!
//! Fiecare nod fizic e plasat de `vnodes` ori pe un inel de 2^64 poziții. O
//! cheie aparține primului punct de pe inel aflat în sensul acelor de ceasornic.
//! Adăugarea/eliminarea unui nod mută doar ~1/N din chei.

/// FNV-1a 64 + finalizator `splitmix64` (distribuție bună, stabil între procese,
/// spre deosebire de `RandomState`).
pub fn stable_hash(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h ^= h >> 30;
    h = h.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    h ^= h >> 27;
    h = h.wrapping_mul(0x94d0_49bb_1331_11eb);
    h ^ (h >> 31)
}

#[derive(Debug, Clone)]
pub struct Ring {
    points: Vec<(u64, usize)>,
}

impl Ring {
    /// `names[i]` identifică nodul `i` (de obicei adresa lui).
    pub fn new<S: AsRef<str>>(names: &[S], vnodes: usize) -> Self {
        let mut points: Vec<(u64, usize)> = names
            .iter()
            .enumerate()
            .flat_map(|(i, n)| {
                (0..vnodes).map(move |v| (stable_hash(format!("{}#{v}", n.as_ref()).as_bytes()), i))
            })
            .collect();
        points.sort_unstable();
        Self { points }
    }

    /// Indexul nodului responsabil de `key`.
    pub fn node_for(&self, key: &str) -> usize {
        assert!(!self.points.is_empty(), "ring has no nodes");
        let h = stable_hash(key.as_bytes());
        let idx = self.points.partition_point(|&(p, _)| p < h);
        self.points[idx % self.points.len()].1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_are_balanced() {
        let ring = Ring::new(&["a:1", "b:1", "c:1"], 200);
        let mut counts = [0usize; 3];
        for i in 0..90_000 {
            counts[ring.node_for(&format!("product:{i}"))] += 1;
        }
        for c in counts {
            assert!((24_000..36_000).contains(&c), "unbalanced: {counts:?}");
        }
    }

    #[test]
    fn adding_a_node_moves_about_one_quarter() {
        let r3 = Ring::new(&["a", "b", "c"], 200);
        let r4 = Ring::new(&["a", "b", "c", "d"], 200);
        let n = 40_000;
        let moved = (0..n)
            .filter(|i| {
                let k = format!("k{i}");
                r3.node_for(&k) != r4.node_for(&k)
            })
            .count();
        let frac = moved as f64 / f64::from(n);
        assert!((0.17..0.33).contains(&frac), "moved {frac}");
    }
}
