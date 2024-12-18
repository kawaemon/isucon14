pub mod deferred;
pub mod models;
pub mod pool;
pub mod ws;

pub type FxHashMap<K, V> = std::collections::HashMap<K, V, fxhash::FxBuildHasher>;
pub type FxHashSet<K> = std::collections::HashSet<K, fxhash::FxBuildHasher>;
