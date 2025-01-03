mod updatable;
use std::sync::LazyLock;

pub use updatable::*;
mod simple;
pub use simple::*;

use tokio::sync::broadcast;
pub static COMMIT_CHAN: LazyLock<(broadcast::Sender<()>, broadcast::Receiver<()>)> =
    LazyLock::new(|| broadcast::channel(16));
