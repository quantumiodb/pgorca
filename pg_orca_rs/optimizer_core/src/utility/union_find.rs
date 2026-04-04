/// Union-Find with path compression and union by rank.
pub struct UnionFind {
    parent: Vec<u32>,
    rank: Vec<u32>,
}

impl UnionFind {
    pub fn new() -> Self {
        Self { parent: Vec::new(), rank: Vec::new() }
    }

    pub fn make_set(&mut self) -> u32 {
        let id = self.parent.len() as u32;
        self.parent.push(id);
        self.rank.push(0);
        id
    }

    pub fn find(&mut self, mut x: u32) -> u32 {
        while self.parent[x as usize] != x {
            self.parent[x as usize] = self.parent[self.parent[x as usize] as usize];
            x = self.parent[x as usize];
        }
        x
    }

    pub fn union(&mut self, a: u32, b: u32) -> u32 {
        let ra = self.find(a);
        let rb = self.find(b);
        if ra == rb { return ra; }
        let (root, child) = if self.rank[ra as usize] >= self.rank[rb as usize] {
            (ra, rb)
        } else {
            (rb, ra)
        };
        self.parent[child as usize] = root;
        if self.rank[root as usize] == self.rank[child as usize] {
            self.rank[root as usize] += 1;
        }
        root
    }

    pub fn same(&mut self, a: u32, b: u32) -> bool {
        self.find(a) == self.find(b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic() {
        let mut uf = UnionFind::new();
        let a = uf.make_set();
        let b = uf.make_set();
        let c = uf.make_set();
        assert!(!uf.same(a, b));
        uf.union(a, b);
        assert!(uf.same(a, b));
        assert!(!uf.same(a, c));
        uf.union(b, c);
        assert!(uf.same(a, c));
    }
}
