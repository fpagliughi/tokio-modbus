// SPDX-FileCopyrightText: Copyright (c) 2017-2026 slowtec GmbH <post@slowtec.de>
// SPDX-License-Identifier: MIT OR Apache-2.0

//! RTU client connections

use crate::{codec, service};
use std::{io, time::Duration};

use tokio::io::{AsyncRead, AsyncWrite};
use tokio_serial::{SerialPort as _, SerialPortBuilder, SerialStream};

use super::*;

/// Open a serial port and connect to no particular Modbus slave device
/// for sending broadcast messages.
pub fn connect(builder: &SerialPortBuilder) -> io::Result<Context> {
    connect_slave(builder, Slave::broadcast())
}

/// Open a serial port and connect to any kind of Modbus slave device.
pub fn connect_slave(builder: &SerialPortBuilder, slave: Slave) -> io::Result<Context> {
    let serial = SerialStream::open(builder)?;
    let baud_rate = serial.baud_rate().map_err(io::Error::other)?;
    let inter_frame_delay = codec::rtu::inter_frame_delay(baud_rate);
    let client = service::rtu::Client::new(serial, slave, inter_frame_delay);
    Ok(Context {
        client: Box::new(client),
    })
}

/// Connect to no particular Modbus slave device for sending
/// broadcast messages.
/// Note that this has no knowledge of the underlying transport, so does not
/// use an inter-frame delay.
pub fn attach<T>(transport: T) -> Context
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    attach_slave(transport, Slave::broadcast())
}

/// Connect to any kind of Modbus slave device.
/// Note that this has no knowledge of the underlying transport, so does not
/// use an inter-frame delay.
pub fn attach_slave<T>(transport: T, slave: Slave) -> Context
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let client = service::rtu::Client::new(transport, slave, Duration::ZERO);
    Context {
        client: Box::new(client),
    }
}
