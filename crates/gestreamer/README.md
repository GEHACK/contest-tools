# gestreamer
This application captures the display and webcam feeds of the system and exposes them as MPEG-TS streams.
Its primary use is streaming live screen video from contestants' screens during ICPC style contests in a format that easily integrates with the [ICPC Contest Data Server](https://tools.icpc.global/cds/).
A predictable [gstreamer](gstreamer.freedesktop.org) pipeline is created which aims for a constant load on the contestant machines.
When used together with eager loading in the CDS, every team machine has the same constant load.

## Desktop sharing
The desktop sharing is implemented using the GNOME Mutter screenshare D-Bus interface, as it allows for starting a sharing session without user interaction.
GNOME is the desktop environment used in the [GEHACK Contest Environment](https://os.gehack.nl/).
Therefore, it is the only implemented desktop environment for desktop screen sharing.

## The gestreamer name
The name is a deliberate misspelling joke on `gstreamer`, as projects from [GEHACK](https://github.com/GEHACK) often start with the `ge` prefix.

## Adding and updating D-Bus interface definitions
The rust bindings for D-Bus interfaces are automatically generated from the bindings introspection definition.
- Save the introspection result to the file `./dbus-introspect/<name>.xml`. When using [genix](https://github.com/GEHACK/genix), you can run this command from within the VM to get up-to-date definitions without installing packages on your host system:
  ```sh
  dbus-send --print-reply --session --dest="<interface>" "<object>" org.freedesktop.DBus.Introspectable.Introspect
  ```
- Run `dbus-codegen-rust -c nonblock --file ./dbus-introspect/<name>.xml --output src/dbus_client/<name>.rs`
- Update the [`src/dbus_client/mod.rs`](./src/dbus_client/mod.rs) definition if needed.
