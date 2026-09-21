use dbus::{
    Path,
    arg::{PropMap, Variant},
    message::SignalArgs,
    nonblock::{MsgMatch, Proxy},
};
use tokio::sync::oneshot;

use crate::dbus_client::screencast_session::OrgGnomeMutterScreenCastSession;
use crate::dbus_client::{DBus, screencast::OrgGnomeMutterScreenCast};
use crate::dbus_client::{
    display_config::OrgGnomeMutterDisplayConfig,
    screencast_stream::OrgGnomeMutterScreenCastStreamPipeWireStreamAdded,
};

pub struct Screencast {
    pub pipewire_node_id: u32,
}

#[repr(u32)]
#[allow(dead_code)]
pub enum CursorMode {
    Hidden = 0,
    Embedded = 1,
    Metadata = 2,
}

pub async fn get_primary_monitor(bus: &DBus) -> Result<String, dbus::Error> {
    let proxy: Proxy<'_, _> = bus.get_displayconfig_proxy();
    let (_serial, _monitors, logical_monitors, _properties) =
        OrgGnomeMutterDisplayConfig::get_current_state(&proxy).await?;

    for (_x, _y, _scale, _transform, primary, monitor_specs, _properties) in logical_monitors {
        if primary {
            let (connector, _vendor, _product, _serial) = &monitor_specs[0];
            return Ok(connector.clone());
        }
    }

    return Err(dbus::Error::new_custom(
        "Monitor",
        "No primary monitor available on the D-Bus interface.",
    ));
}

pub async fn start_screencast(bus: &DBus, connector: &str) -> Result<u32, dbus::Error> {
    let screencast_proxy: Proxy<'_, _> = bus.get_screencast_proxy();
    let session_path: Path<'_> = screencast_proxy.create_session(PropMap::new()).await?;
    println!("Session created: {session_path:?}");
    let session_proxy: Proxy<'_, _> = bus.get_screencast_proxy_with_path(session_path);

    let mut properties: PropMap = PropMap::new();
    properties.insert(
        String::from("cursor-mode"),
        Variant(Box::new(CursorMode::Embedded as u32)),
    );
    properties.insert(String::from("is-recording"), Variant(Box::new(true)));
    let stream_path: Path<'_> = session_proxy.record_monitor(connector, properties).await?;
    println!("Stream created: {stream_path:?}");

    let (tx, rx) = oneshot::channel();
    let rule =
        OrgGnomeMutterScreenCastStreamPipeWireStreamAdded::match_rule(None, Some(&stream_path));
    let msg_match: MsgMatch = bus.conn.add_match(rule.static_clone()).await?;
    let _msg_match = msg_match.cb({
        let mut tx = Some(tx);
        move |_: dbus::Message, signal: OrgGnomeMutterScreenCastStreamPipeWireStreamAdded| {
            if let Some(tx) = tx.take() {
                let _ = tx.send(signal.node_id);
            }
            // Remove the match after the first matching signal.
            return false;
        }
    });

    session_proxy.start().await?;
    let pipewire_node_id = rx.await.map_err(|err| {
        dbus::Error::new_custom("ScreenCast", &format!("Failed to start screencast: {err}"))
    })?;

    println!("PipeWire screencast stream added: {}", pipewire_node_id);
    return Ok(pipewire_node_id);
}

impl Screencast {
    pub async fn start(bus: &DBus) -> Result<Screencast, dbus::Error> {
        let connector = get_primary_monitor(bus).await?;
        let node_id = start_screencast(bus, connector.as_str()).await?;
        return Ok(Screencast {
            pipewire_node_id: node_id,
        });
    }
}
