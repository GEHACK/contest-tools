use serde::Deserialize;
use shared::{ConfigLoader, setup_logging};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    test: String,
}

fn main() {
    setup_logging!("info");
    let config = match ConfigLoader::new().load::<Config>("test.toml") {
        Ok(config) => config,
        Err(e) => {
            tracing::error!(error=%e, "failed to load config");
            return;
        }
    };
    tracing::info!(test = &config.test, "config loaded successful")
}
