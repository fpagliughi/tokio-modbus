// SPDX-FileCopyrightText: Copyright (c) 2017-2026 slowtec GmbH <post@slowtec.de>
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Modbus RTU server skeleton

use std::{future::Future, io, path::Path, time::Duration};

use futures_util::{FutureExt as _, SinkExt as _, StreamExt as _};
use tokio::time::Instant;
use tokio_serial::{SerialPort as _, SerialStream};
use tokio_util::codec::Framed;

use crate::{
    codec::rtu::{self, ServerCodec},
    frame::{
        ExceptionResponse, OptionalResponsePdu, RequestPdu,
        rtu::{RequestAdu, ResponseAdu},
    },
    slave::Slave,
};

use super::{Service, Terminated};

#[derive(Debug)]
pub struct Server {
    serial: SerialStream,
    inter_frame_delay: Duration,
}

impl Server {
    /// set up a new [`Server`] instance from an interface path and baud rate
    pub fn new_from_path<P: AsRef<Path>>(p: P, baud_rate: u32) -> io::Result<Self> {
        let serial =
            SerialStream::open(&tokio_serial::new(p.as_ref().to_string_lossy(), baud_rate))?;
        let inter_frame_delay = rtu::inter_frame_delay(baud_rate);
        Ok(Server {
            serial,
            inter_frame_delay,
        })
    }

    /// set up a new [`Server`] instance based on a pre-configured [`SerialStream`] instance
    #[must_use]
    pub fn new(serial: SerialStream) -> Self {
        let inter_frame_delay = serial
            .baud_rate()
            .map(rtu::inter_frame_delay)
            .unwrap_or(rtu::DEFAULT_INTER_FRAME_DELAY);
        Server {
            serial,
            inter_frame_delay,
        }
    }

    /// Process Modbus RTU requests.
    pub async fn serve_forever<S>(self, service: S) -> io::Result<()>
    where
        S: Service + Send + Sync + 'static,
        S::Request: From<RequestAdu<'static>> + Send,
    {
        let framed = Framed::new(self.serial, ServerCodec::default());
        process(framed, service, self.inter_frame_delay).await
    }

    /// Process Modbus RTU requests until finished or aborted.
    ///
    /// Warning: Request processing is not scoped and could be aborted at any internal await point!
    /// See also: <https://rust-lang.github.io/wg-async/vision/roadmap/scopes.html#cancellation>
    pub async fn serve_until<S, X>(self, service: S, abort_signal: X) -> io::Result<Terminated>
    where
        S: Service + Send + Sync + 'static,
        S::Request: From<RequestAdu<'static>> + Send,
        X: Future<Output = ()> + Sync + Send + 'static,
    {
        let framed = Framed::new(self.serial, ServerCodec::default());
        let abort_signal = abort_signal.fuse();
        tokio::select! {
            res = process(framed, service, self.inter_frame_delay) => {
                res.map(|()| Terminated::Finished)
            },
            () = abort_signal => {
                Ok(Terminated::Aborted)
            }
        }
    }
}

/// frame wrapper around the underlying service's responses to forwarded requests
async fn process<S>(
    mut framed: Framed<SerialStream, ServerCodec>,
    service: S,
    inter_frame_delay: Duration,
) -> io::Result<()>
where
    S: Service + Send + Sync + 'static,
    S::Request: From<RequestAdu<'static>> + Send,
{
    let mut last_bus_activity: Option<Instant> = None;

    loop {
        let Some(request_adu) = framed.next().await.transpose().inspect_err(|err| {
            log::debug!("Failed to receive and decode request ADU: {err}");
        })?
        else {
            log::debug!("Stream has finished");
            break;
        };
        let received_at = Instant::now();

        let RequestAdu {
            hdr,
            pdu: RequestPdu(request),
        } = &request_adu;
        let hdr = *hdr;
        let fc = request.function_code();
        let slave = Slave::from(hdr.slave_id);
        let OptionalResponsePdu(Some(response_pdu)) = service
            .call(request_adu.into())
            .await
            .map(Into::into)
            .map_err(|e| ExceptionResponse {
                function: fc,
                exception: e.into(),
            })
            .into()
        else {
            log::trace!("No response for request {hdr:?} (function = {fc})");
            last_bus_activity = Some(received_at);
            continue;
        };

        // Enforce t3.5 inter-frame silence before sending response.
        // Use the most recent bus activity (receive or previous send).
        let reference = last_bus_activity.map_or(received_at, |last| last.max(received_at));
        let elapsed = reference.elapsed();
        if elapsed < inter_frame_delay {
            tokio::time::sleep(inter_frame_delay.saturating_sub(elapsed)).await;
        }

        // For RTU, broadcast requests (SlaveId=0) are processed but
        // must not put a response on the bus.
        if slave.is_broadcast() {
            log::trace!("Not sending response for broadcast request (function = {fc})");
        } else {
            framed
                .send(ResponseAdu {
                    hdr,
                    pdu: response_pdu,
                })
                .await
                .inspect_err(|err| {
                    log::debug!(
                        "Failed to send response for request {hdr:?} (function = {fc}): {err}"
                    );
                })?;
        }
    }
    Ok(())
}
