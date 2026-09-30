use std::net::IpAddr;

use clap::{ArgAction, Parser};

/// Stream screencast and webcam using MPEG-TS
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    #[arg(short, long, default_value_t = IpAddr::from([0, 0, 0, 0]))]
    pub bind_address: IpAddr,
    #[arg(short, long, default_value_t = 8080)]
    pub port: u16,
    #[arg(short, long, default_value_t = true, action = ArgAction::Set)]
    pub screencast: bool,
    #[arg(short, long, default_value = None)]
    pub webcam: Option<String>,
    #[arg(short, long, default_value_t = String::from("x264enc key-int-max=12 ! h264parse"))]
    pub encoder: String,
}
