use std::{mem::MaybeUninit, sync::RwLock};

use actix_web::web::Bytes;
use gstreamer::Sample;
use tokio::sync::broadcast::{self, Receiver, Sender};
use tokio_stream::wrappers::BroadcastStream;

pub const MPEGTS_PACKET_SIZE: usize = 188;
pub const PACKETS_IN_BUFFER: usize = 7;
pub const MPEGTS_BUFFER_SIZE: usize = MPEGTS_PACKET_SIZE * PACKETS_IN_BUFFER;
pub const CHANNEL_SIZE: usize = 512;

pub type MpegTsBuffer = Bytes;

pub struct PipelineDistributor {
    sender: Sender<MpegTsBuffer>,
    pub is_running: RwLock<bool>,
}

impl PipelineDistributor {
    pub fn new() -> Box<PipelineDistributor> {
        let (tx, _) = broadcast::channel::<MpegTsBuffer>(CHANNEL_SIZE);
        let pd = Box::new(PipelineDistributor {
            sender: tx,
            is_running: RwLock::new(false)
        });
        return pd;
    }

    pub fn create_stream(&self) -> BroadcastStream<Bytes> {
        let rx: Receiver<MpegTsBuffer> = self.sender.subscribe();
        return BroadcastStream::new(rx);
    }

    pub fn write_sample(&self, sample: &Sample) {
        if let Some(buffer) = sample.buffer_owned() {
            // We should be receiving buffers sized MPEGTS_BUFFER_SIZE in the app sink.
            assert_eq!(buffer.size(), MPEGTS_BUFFER_SIZE);

            let mut copy: Vec<u8> = Vec::with_capacity(MPEGTS_BUFFER_SIZE);
            let dest: *mut MaybeUninit<u8> = copy.spare_capacity_mut().as_mut_ptr();
            let dest = dest as gstreamer::glib::ffi::gpointer;
            unsafe {
                let copied: usize = gstreamer::ffi::gst_buffer_extract(
                    buffer.as_mut_ptr(),
                    0,
                    dest,
                    MPEGTS_BUFFER_SIZE,
                );
                if copied < MPEGTS_BUFFER_SIZE {
                    println!(
                        "Copied only {} bytes, zeroing {} bytes",
                        copied,
                        MPEGTS_BUFFER_SIZE - copied
                    );
                    // Safety: initialize the remaining memory before it is read.
                    copy[copied..MPEGTS_BUFFER_SIZE].fill(0);
                }
                copy.set_len(MPEGTS_BUFFER_SIZE);
            }
            let bytes = Bytes::from_owner(copy);
            // Ignore no receiver errors.
            let _ = self.sender.send(bytes);
        } else {
            eprintln!("Failed to get sample buffer");
        }
    }
}
