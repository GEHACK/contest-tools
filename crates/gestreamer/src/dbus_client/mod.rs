use std::{sync::Arc, time::Duration};

use dbus::{Path, nonblock::{Proxy, SyncConnection}};
use dbus_tokio::connection;
use tokio::task::JoinHandle;

pub mod display_config;
pub mod screencast;
pub mod screencast_session;
pub mod screencast_stream;

const DISPLAYCONFIG_DEST: &'static str = "org.gnome.Mutter.DisplayConfig";
const DISPLAYCONFIG_PATH: &'static str = "/org/gnome/Mutter/DisplayConfig";

const SCREENCAST_DEST: &'static str = "org.gnome.Mutter.ScreenCast";
const SCREENCAST_PATH: &'static str = "/org/gnome/Mutter/ScreenCast";

pub struct DBus {
    pub conn: Arc<SyncConnection>,
    handle: JoinHandle<()>,
}

impl DBus {
    pub async fn connect() -> Result<DBus, dbus::Error> {
        let (resource, conn) = connection::new_session_sync()?;
        let _handle = tokio::spawn(async {
            let err = resource.await;
            panic!("Lost connection to D-Bus: {}", err);
        });

        return Ok(DBus {
            conn: conn,
            handle: _handle,
        });
    }

    pub fn get_displayconfig_proxy(&self) -> Proxy<'_, Arc<SyncConnection>> {
        Proxy::new(
            DISPLAYCONFIG_DEST,
            DISPLAYCONFIG_PATH,
            Duration::from_secs(1),
            Arc::clone(&self.conn),
        )
    }

    pub fn get_screencast_proxy(&self) -> Proxy<'_, Arc<SyncConnection>> {
        self.get_screencast_proxy_with_path(SCREENCAST_PATH.into())
    }

    pub fn get_screencast_proxy_with_path<'a>(&self, path: Path<'a>) -> Proxy<'a, Arc<SyncConnection>> {
        Proxy::new(
            SCREENCAST_DEST,
            path,
            Duration::from_secs(1),
            Arc::clone(&self.conn),
        )
    }

    pub async fn disconnect(&self) {
        self.handle.abort();
    }
}
