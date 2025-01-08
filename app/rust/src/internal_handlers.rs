use std::time::Duration;

use crate::AppState;

crate::conf_env!(static MATCHING_INTERVAL_MS: u64 = {
    from: "MATCHING_INTERVAL_MS",
    default: "100",
});

pub fn spawn_matching_thread(state: AppState) {
    tokio::spawn(async move {
        loop {
            state.repo.do_matching();
            tokio::time::sleep(Duration::from_millis(*MATCHING_INTERVAL_MS)).await;
        }
    });
}

// interval=50,  tick=600, req=491
// interval=75,  tick=600, req=2455
// interval=100, tick=600, req=2558
// interval=150, tick=600, req=2444
