pub type FxHashMap<K, V> = std::collections::HashMap<K, V, fxhash::FxBuildHasher>;
pub type FxHashSet<K> = std::collections::HashSet<K, fxhash::FxBuildHasher>;

pub mod deferred;
