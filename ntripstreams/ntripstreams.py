#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: Lars Stenseng
@mail: lars@stenseng.net
"""

import asyncio
import logging
from base64 import b64encode
from time import gmtime, strftime, time
from urllib.parse import urlsplit

from bitstring import Bits, BitStream
from ntripstreams.__version__ import __version__
from ntripstreams.crc import crc24q


class NtripStream:
    logger = logging.getLogger('ntripstreams')
    rx_logger = logging.getLogger('ntripstreams.receive')

    def __init__(self):
        self.__CLIENTVERSION = __version__
        self.__CLIENTNAME = "Bedrock Solutions NtripClient/" + f"{self.__CLIENTVERSION}"
        self.casterUrl = None
        self.ntripWriter = None
        self.ntripReader = None
        self.ntripVersion = 2
        self.ntripMountPoint = None
        self.ntripAuthString = ""
        self.ntripRequestHeader = ""
        self.ntripResponseHeader = []
        self.ntripResponseStatusCode = None
        self.ntripStreamChunked = False
        self.nmeaString = ""
        self.rtcmFrameBuffer = BitStream()
        self.rtcmFramePreample = False
        self.rtcmFrameAligned = False

    async def openNtripConnection(self, casterUrl: str):
        """
        Connects to a caste with url http[s]://caster.hostename.net:port
        """
        self.casterUrl = urlsplit(casterUrl)
        if self.casterUrl.scheme == "https":
            self.ntripReader, self.ntripWriter = await asyncio.open_connection(
                self.casterUrl.hostname, self.casterUrl.port, ssl=True
            )
        else:
            self.ntripReader, self.ntripWriter = await asyncio.open_connection(
                self.casterUrl.hostname, self.casterUrl.port
            )

    async def closeNtripConnection(self):
        if self.ntripWriter is not None:
            self.ntripWriter.close()
            await self.ntripWriter.wait_closed()
            self.ntripWriter = None
            self.ntripReader = None

    def setRequestSourceTableHeader(self, casterUrl: str):
        self.casterUrl = urlsplit(casterUrl)
        timestamp = strftime("%a, %d %b %Y %H:%M:%S GMT", gmtime())
        self.ntripRequestHeader = (
            f"GET / HTTP/1.1\r\n"
            f"Host: {self._get_host_value(self.casterUrl)}\r\n"
            f"Ntrip-Version: Ntrip/"
            f"{self.ntripVersion}.0\r\n"
            f"User-Agent: NTRIP {self.__CLIENTNAME}\r\n"
            f"Date: {timestamp}\r\n"
            f"Connection: close\r\n"
            f"\r\n"
        ).encode("ISO-8859-1")

    def setRequestStreamHeader(
        self,
        casterUrl: str,
        ntripMountPoint: str,
        ntripUser: str = None,
        ntripPassword: str = None,
        nmeaString: str = None,
    ):
        self.casterUrl = urlsplit(casterUrl)
        self.ntripMountPoint = ntripMountPoint
        timestamp = strftime("%a, %d %b %Y %H:%M:%S GMT", gmtime())
        if nmeaString:
            self.nmeaString = nmeaString.encode("ISO-8859-1")
        if ntripUser and ntripPassword:
            ntripAuth = b64encode(
                (ntripUser + ":" + ntripPassword).encode("ISO-8859-1")
            ).decode()
            self.ntripAuthString = f"Authorization: Basic {ntripAuth}\r\n"
        self.ntripRequestHeader = (
            f"GET /{ntripMountPoint} HTTP/1.1\r\n"
            f"Host: {self._get_host_value(self.casterUrl)}\r\n"
            "Ntrip-Version: Ntrip/"
            f"{self.ntripVersion}.0\r\n"
            f"User-Agent: NTRIP {self.__CLIENTNAME}\r\n"
            + self.ntripAuthString
            + self.nmeaString
            + f"Date: {timestamp}\r\n"
            "Connection: close\r\n"
            "\r\n"
        ).encode("ISO-8859-1")

    def setRequestServerHeader(
        self,
        casterUrl: str,
        ntripMountPoint: str,
        ntripUser: str = None,
        ntripPassword: str = None,
        ntripVersion: int = 2,
    ):
        self.casterUrl = urlsplit(casterUrl)
        if ntripVersion == 1:
            self.ntripVersion = 1
        timestamp = strftime("%a, %d %b %Y %H:%M:%S GMT", gmtime())

        if self.ntripVersion == 2:
            if ntripUser and ntripPassword:
                ntripAuth = b64encode(
                    (ntripUser + ":" + ntripPassword).encode("ISO-8859-1")
                ).decode()
            self.ntripAuthString = f"Authorization: Basic {ntripAuth}\r\n"
            self.ntripRequestHeader = (
                f"POST /{ntripMountPoint} HTTP/1.1\r\n"
                f"Host: {self._get_host_value(self.casterUrl)}\r\n"
                "Ntrip-Version: Ntrip/"
                f"{self.ntripVersion}.0\r\n"
                + self.ntripAuthString
                + "User-Agent: NTRIP "
                f"{self.__CLIENTNAME}\r\n"
                f"Date: {timestamp}\r\n"
                "Connection: close\r\n"
                "\r\n"
            ).encode("ISO-8859-1")
        elif self.ntripVersion == 1:
            if ntripPassword:
                ntripAuth = b64encode(ntripPassword.encode("ISO-8859-1")).decode()
            self.ntripRequestHeader = (
                f"SOURCE {ntripAuth} "
                f"/{ntripMountPoint} HTTP/1.1\r\n"
                "Source-Agent: NTRIP "
                f"{self.__CLIENTNAME}\r\n"
                "\r\n"
            ).encode("ISO-8859-1")

    async def getNtripResponseHeader(self):
        self.ntripResponseHeader = []
        ntripResponseHeaderTimestamp = []
        endOfHeader = False
        while True:
            line = await self.ntripReader.readline()
            ntripResponseHeaderTimestamp.append(time())
            if not line:
                break
            line = line.decode("ISO-8859-1").rstrip()
            if line == "":
                endOfHeader = True
                break
            if not endOfHeader:
                if "Transfer-Encoding: chunked".lower() in str.lower(line):
                    self.ntripStreamChunked = True
                    self.logger.debug(f"{self.ntripMountPoint}:Stream is chunked")
                self.ntripResponseHeader.append(line)
        for line in self.ntripResponseHeader:
            self.logger.debug(f"{self.ntripMountPoint}:TCP response: {line}")
        statusResponse = self.ntripResponseHeader[0].split(" ")
        if len(statusResponse) > 1:
            self.ntripResponseStatusCode = statusResponse[1]
        else:
            self.ntripResponseStatusCode = 0

    def ntripResponseStatusOk(self):
        if self.ntripResponseStatusCode == "200":
            self.rtcmFramePreample = False
            self.rtcmFrameAligned = False
            return True
        else:
            self.logger.error(
                f"{self.ntripMountPoint}:Response error "
                f"{self.ntripResponseStatusCode}!"
            )
            for line in self.ntripResponseHeader:
                self.logger.error(f"{self.ntripMountPoint}:TCP response: {line}")
            raise ConnectionRefusedError(
                f"{self.ntripMountPoint}:" f"{self.ntripResponseHeader[0]}"
            ) from None
            self.ntripWriter.close()
            return False

    async def requestSourcetable(self, casterUrl: str):
        await self.openNtripConnection(casterUrl)
        self.logger.debug(f"Connection to {casterUrl} open. Ready to write.")
        self.setRequestSourceTableHeader(self.casterUrl.geturl())
        self.ntripWriter.write(self.ntripRequestHeader)
        await self.ntripWriter.drain()
        self.logger.debug("Sourcetable request sent.")
        ntripSourcetable = []
        await self.getNtripResponseHeader()
        if self.ntripResponseStatusCode != "200":
            self.logger.error(f"Response error {self.ntripResponseStatusCode}!")
            for line in self.ntripResponseHeader:
                self.logger.error(f"TCP response: {line}")
            self.ntripWriter.close()
            raise ConnectionRefusedError(
                f"{self.casterUrl.geturl()}:" f"{self.ntripResponseHeader[0]}"
            ) from None
        while True:
            line = await self.ntripReader.readline()
            if not line:
                break
            line = line.decode("ISO-8859-1").rstrip()
            if line == "ENDSOURCETABLE":
                ntripSourcetable.append(line)
                self.ntripWriter.close()
                self.logger.debug("Sourcetabel received.")
                break
            else:
                ntripSourcetable.append(line)
        return ntripSourcetable

    async def requestNtripServer(
        self,
        casterUrl: str,
        mountPoint: str,
        user: str = None,
        passwd: str = None,
        ntripVersion: int = 2,
    ):
        self.ntripVersion = ntripVersion
        await self.openNtripConnection(casterUrl)
        self.ntripMountPoint = mountPoint
        self.logger.debug(
            f"{self.ntripMountPoint}:Connection to {casterUrl} open. " "Ready to write."
        )
        self.setRequestServerHeader(
            self.casterUrl.geturl(), self.ntripMountPoint, user, passwd
        )
        self.ntripWriter.write(self.ntripRequestHeader)
        await self.ntripWriter.drain()
        self.logger.debug(f"{self.ntripMountPoint}:Request server header sent.")
        await self.getNtripResponseHeader()
        self.ntripResponseStatusOk()

    async def sendRtcmFrame(self, rtcmFrame):
        self.ntripWriter.write(rtcmFrame)
        await self.ntripWriter.drain()

    async def requestNtripStream(
        self,
        casterUrl: str,
        mountPoint: str,
        user: str = None,
        passwd: str = None,
        ntripVersion: int = 2,
    ):
        self.ntripVersion = ntripVersion
        await self.openNtripConnection(casterUrl)
        self.ntripMountPoint = mountPoint
        self.logger.debug(
            f"{self.ntripMountPoint}:Connection to {casterUrl} open. " "Ready to write."
        )
        self.setRequestStreamHeader(
            self.casterUrl.geturl(), self.ntripMountPoint, user, passwd
        )
        self.ntripWriter.write(self.ntripRequestHeader)
        await self.ntripWriter.drain()
        self.logger.debug(f"{self.ntripMountPoint}:Request stream header sent.")
        await self.getNtripResponseHeader()
        self.ntripResponseStatusOk()

    async def getRawData(self, max_length=1024):
        # If we're receiving chunked data, wait for one chunk.
        if self.ntripStreamChunked:
            # Read the chunk length.
            rawLine = await self.ntripReader.readuntil(b"\r\n")
            chunk_length = int(rawLine[:-2].decode("ISO-8859-1"), 16)

            # Now read the contents. Chunks all terminate with a \r\n, but it's not sufficient to do a readuntil() since
            # \r\n could appear legitimately within the RTCM data stream.
            rawLine = await self.ntripReader.readexactly(chunk_length + 2)
            data = rawLine[:-2]

            self.rx_logger.debug(f"Chunk {len(data)} bytes.")
            return data
        # Otherwise, perform a single read up to the specified max length.
        else:
            data = await self.ntripReader.read(max_length)
            self.rx_logger.debug(f"Read {len(data) * 8} bits.")
            return data

    async def getRtcmFrame(self):
        rtcm3FramePreample = Bits(bin="0b11010011")
        rtcm3FrameHeaderFormat = "bin:8, pad:6, uint:10"
        rtcmFrameComplete = False
        while not rtcmFrameComplete:
            data = await self.getRawData()
            timeStamp = time()
            receivedBytes = BitStream(data)
            self.rtcmFrameBuffer += receivedBytes

            if not self.rtcmFrameAligned:
                rtcmFramePos = self.rtcmFrameBuffer.find(
                    rtcm3FramePreample, bytealigned=True
                )
                if rtcmFramePos:
                    firstFrame = rtcmFramePos[0]
                    self.rtcmFrameBuffer = self.rtcmFrameBuffer[firstFrame:]
                    self.rtcmFramePreample = True
                else:
                    self.rtcmFrameBuffer = BitStream()
            if self.rtcmFramePreample and self.rtcmFrameBuffer.length >= 48:
                (rtcmPreAmple, rtcmPayloadLength) = self.rtcmFrameBuffer.peeklist(
                    rtcm3FrameHeaderFormat
                )
                rtcmFrameLength = (rtcmPayloadLength + 6) * 8
                if self.rtcmFrameBuffer.length >= rtcmFrameLength:
                    rtcmFrame = self.rtcmFrameBuffer[:rtcmFrameLength]
                    calcCrc = crc24q(rtcmFrame[:-24])
                    frameCrc = rtcmFrame[-24:].unpack("uint:24")
                    if calcCrc == frameCrc[0]:
                        self.rtcmFrameAligned = True
                        self.rtcmFrameBuffer = self.rtcmFrameBuffer[rtcmFrameLength:]
                        rtcmFrameComplete = True
                    else:
                        self.rtcmFrameAligned = False
                        self.rtcmFrameBuffer = self.rtcmFrameBuffer[8:]
                        self.logger.warning(
                            f"{self.ntripMountPoint}:CRC mismatch "
                            f"{hex(calcCrc)} != {rtcmFrame[-24:]}."
                            f" Realigning!"
                        )
        return rtcmFrame, timeStamp

    def _get_host_value(self, url):
        if url.port is None:
            return url.hostname
        else:
            return f"{url.hostname}:{url.port}"
