use std::sync::{Arc, Mutex, Once};

use futures::StreamExt as _;
use libshvgate_rs::{JournalConfig, ShvGate, ShvGateConfig, ShvTreeDefinition};
use log::LevelFilter;
use shvclient::clientnode::METH_GET;
use shvrpc::rpcmessage::{RpcError, RpcErrorCode};

#[derive(Debug)]
struct SimConfig {
    base_temp: f64,
    drift: f64,
    period_ms: u64,
}

fn init_logger() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        simple_logger::SimpleLogger::new()
            .with_level(LevelFilter::Debug)
            .init()
            .unwrap();
    });
}

fn gate_config() -> ShvGateConfig {
    const TREE_YAML: &str = r#"
version: "1"

nodes:
  device/temperature: Temperature
  device/status: Status
  device: Device

node_methods:
  Device:
    - name: config
      access: wr
      flags: [UserIDRequired]
      param: RpcMap

  Temperature:
    - name: get
      access: rd
      flags: [IsGetter]
      signals: {chng: null}
      result: Float

  Status:
    - name: get
      access: rd
      flags: [IsGetter]
      signals: {chng: null}
      result: Bool
"#;

    let tree = ShvTreeDefinition::from_yaml(TREE_YAML);

    ShvGateConfig {
        tree,
        journal: JournalConfig {
            root_path: "journal3_example".into(),
            max_file_entries: 100,
            max_journal_size: 100_000,
        }
    }
}

#[tokio::main]
async fn main() {
    init_logger();

    log::info!("Starting example SHV device...");

    let sim_cfg = Arc::new(Mutex::new(SimConfig {
        base_temp: 21.5,
        drift: 0.3,
        period_ms: 2000,
    }));

    let broker_url =
        std::env::var("BROKER_URL")
        .expect("BROKER_URL env variable must be set");

    let client_config = shvclient::shvrpc::client::ClientConfig {
        url: url::Url::parse(&broker_url).expect("valid broker URL"),
        ..Default::default()
    };

    let sim_cfg_clone = sim_cfg.clone();
    ShvGate::new(gate_config())
        .await
        .with_method_call_handler(move |_path, method, value, _client_cmd_tx, _gate_data| {
            let sim_cfg = sim_cfg_clone.clone();
            async move {
                if method == "config" {
                    let mut cfg = sim_cfg.lock().unwrap();

                    if let Some(v) = value {
                        let map = v.as_map();
                        if let Some(v) = map.get("base_temp").map(|v| v.as_decimal().to_f64()) {
                            cfg.base_temp = v;
                        }
                        if let Some(v) = map.get("drift").map(|v| v.as_decimal().to_f64()) {
                            cfg.drift = v;
                        }
                        if let Some(v) = map.get("period_ms").map(|v| v.as_u64()) {
                            cfg.period_ms = v;
                        }
                    }

                    log::info!("New sim config: {:?}", *cfg);

                    return Ok(format!(
                            "config updated: base_temp={}, drift={}, period_ms={}",
                            cfg.base_temp, cfg.drift, cfg.period_ms
                    ));
                }

                Err(RpcError::new(
                        RpcErrorCode::MethodNotFound,
                        "Unknown method",
                ))
            }
        })
        .run(&client_config, move |ccs, mut cer, gate_data| {
            shvclient::runtime::spawn_task(async move {
                // Wait until connected
                while let Some(event) = cer.next().await {
                    if matches!(event, shvclient::ClientEvent::Connected(_)) {
                        log::info!("Connected to broker");
                        break;
                    }
                }

                let mut temp: f64;
                let mut status = false;
                loop {
                    let (base, drift, period) = {
                        let cfg = sim_cfg.lock().unwrap();
                        let base = cfg.base_temp;
                        let drift = cfg.drift;
                        let period = cfg.period_ms;
                        (base, drift, period)
                    };

                    temp = base + (rand::random::<f64>() - 0.5) * drift;
                    status = !status;

                    log::debug!("Sim temp={temp:.2}, status={status}");

                    gate_data.update_value(
                        "device/temperature",
                        METH_GET,
                        temp.into(),
                        false,
                        &ccs,
                    ).await.ok();

                    gate_data.update_value(
                        "device/status",
                        METH_GET,
                        status.into(),
                        false,
                        &ccs,
                    ).await.ok();

                    tokio::time::sleep(std::time::Duration::from_millis(period)).await;
                }
            }).detach();
        })
        .await
        .unwrap();
    }
