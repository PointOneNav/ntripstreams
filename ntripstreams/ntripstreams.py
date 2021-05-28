#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: Lars Stenseng
@mail: lars@stenseng.net
"""

import asyncio
from urllib.parse import urlsplit
from base64 import b64encode
from time import time, strftime, gmtime
from bitstring import Bits, BitStream
from crc import crc24q

from __version__ import __version__


class NtripStream:

    def __init__(self):
        self.__CLIENTVERSION = __version__
        self.__CLIENTNAME = ('Bedrock Solutions NtripClient/'
                             + f'{self.__CLIENTVERSION}')
        self.casterUrl = None
        self.ntripWriter = None
        self.ntripReader = None
        self.ntripVersion = 2
        self.ntripMountPoint = None
        self.ntripAuthString = ''
        self.ntripRequestHeader = ''
        self.ntripResponseHeader = []
        self.ntripResponseStatusCode = None
        self.ntripStreamChunked = False
        self.nmeaString = ''
        self.rtcmFrameBuffer = BitStream()
        self.rtcmFramePreample = False
        self.rtcmFrameAligned = False

    async def openNtripConnection(self, casterUrl: str):
        """
        Connects to a caste with url http[s]://caster.hostename.net:port
        """
        self.casterUrl = urlsplit(casterUrl)
        if self.casterUrl.scheme == 'https':
            self.ntripReader, self.ntripWriter = await asyncio.open_connection(
                self.casterUrl.hostname, self.casterUrl.port, ssl=True)
        else:
            self.ntripReader, self.ntripWriter = await asyncio.open_connection(
                self.casterUrl.hostname, self.casterUrl.port)

    def setRequestSourceTableHeader(self, casterUrl: str) -> str:
        self.casterUrl = urlsplit(casterUrl)
        timestamp = strftime("%a, %d %b %Y %H:%M:%S GMT", gmtime())
        self.ntripRequestHeader = (f'GET / HTTP/1.1\r\n'
                                   f'Host: {self.casterUrl.geturl()}\r\n'
                                   f'Ntrip-Version: Ntrip/'
                                   f'{self.ntripVersion}.0\r\n'
                                   f'User-Agent: NTRIP {self.__CLIENTNAME}\r\n'
                                   f'Date: {timestamp}\r\n'
                                   f'Connection: close\r\n'
                                   f'\r\n').encode('ISO-8859-1')
        return self.ntripRequestHeader

    def setRequestStreamHeader(self, casterUrl: str, ntripMountPoint: str,
                               ntripUser: str = None,
                               ntripPassword: str = None,
                               nmeaString: str = None) -> str:
        self.casterUrl = urlsplit(casterUrl)
        self.ntripMountPoint = ntripMountPoint
        timestamp = strftime("%a, %d %b %Y %H:%M:%S GMT", gmtime())
        if nmeaString:
            self.nmeaString = nmeaString.encode('ISO-8859-1')
        if ntripUser and ntripPassword:
            ntripAuth = b64encode((ntripUser + ':' +
                                   ntripPassword).encode('ISO-8859-1')
                                  ).decode()
            self.ntripAuthString = f'Authorization: Basic {ntripAuth}\r\n'
        self.ntripRequestHeader = (f'GET /{ntripMountPoint} HTTP/1.1\r\n'
                                   f'Host: {self.casterUrl.geturl()}\r\n'
                                   'Ntrip-Version: Ntrip/'
                                   f'{self.ntripVersion}.0\r\n'
                                   f'User-Agent: NTRIP {self.__CLIENTNAME}\r\n'
                                   + self.ntripAuthString
                                   + self.nmeaString +
                                   f'Date: {timestamp}\r\n'
                                   'Connection: close\r\n'
                                   '\r\n').encode('ISO-8859-1')
        return self.ntripRequestHeader

    def setRequestServerHeader(self, casterUrl: str, ntripMountPoint: str,
                               ntripUser: str = None,
                               ntripPassword: str = None,
                               ntripVersion: int = 2) -> str:
        self.casterUrl = urlsplit(casterUrl)
        timestamp = strftime("%a, %d %b %Y %H:%M:%S GMT", gmtime())

        if ntripVersion >= 2.0:
            ntripAuth = b64encode((ntripUser + ':' +
                                   ntripPassword).encode('ISO-8859-1')
                                  ).decode()
            self.ntripAuthString = f'Authorization: Basic {ntripAuth}\r\n'
            self.ntripRequestHeader = (f'POST /{ntripMountPoint} HTTP/1.1\r\n'
                                       f'Host: {self.casterUrl.geturl()}\r\n'
                                       'Ntrip-Version: Ntrip/'
                                       f'{self.ntripVersion}.0\r\n'
                                       + self.ntripAuthString +
                                       'User-Agent: NTRIP '
                                       f'{self.__CLIENTNAME}\r\n'
                                       f'Date: {timestamp}\r\n'
                                       'Connection: close\r\n'
                                       '\r\n').encode('ISO-8859-1')
        elif ntripVersion == 1.0:
            ntripAuth = b64encode(ntripPassword.encode('ISO-8859-1')).decode()
            self.ntripRequestHeader = (f'SOURCE {ntripAuth} '
                                       f'/{ntripMountPoint} HTTP/1.1\r\n'
                                       'Source-Agent: NTRIP '
                                       f'{self.__CLIENTNAME}\r\n'
                                       '\r\n').encode('ISO-8859-1')
        return self.ntripRequestHeader

    async def getNtripResponceHeader(self):
        self.ntripResponceHeader = []
        ntripResponceHeaderTimestamp = []
        endOfHeader = False
        while True:
            line = await self.ntripReader.readline()
            ntripResponceHeaderTimestamp.append(time())
            if not line:
                break
            line = line.decode('ISO-8859-1').rstrip()
            if line == '':
                endOfHeader = True
                break
            if not endOfHeader:
                self.ntripResponceHeader.append(line)
        self.ntripResponseStatusCode \
            = self.ntripResponceHeader[0].split(' ')[1]

    async def requestSourcetable(self, casterUrl: str):
        await self.openNtripConnection(casterUrl)
        print(f'{time():.6f}: Connection open. Ready to write.')
        self.ntripRequestHeader \
            = self.setRequestSourceTableHeader(self.casterUrl.geturl())
        self.ntripWriter.write(self.ntripRequestHeader)
        await self.ntripWriter.drain()
        print(f'{time():.6f}: Request sent.')
        ntripSourcetable = []
        await self.getNtripResponceHeader()
        if self.ntripResponseStatusCode != '200':
            print(f'Status code error! {self.ntripResponseStatusCode}')
            for line in self.ntripResponceHeader:
                print(f'Debug: {line}')
            self.ntripWriter.close()
        while True:
            line = await self.ntripReader.readline()
            if not line:
                break
            line = line.decode('ISO-8859-1').rstrip()
            if line == 'ENDSOURCETABLE':
                ntripSourcetable.append(line)
                self.ntripWriter.close()
                print(f'{time():.6f}: Sourcetabel received.')
                break
            else:
                ntripSourcetable.append(line)
        return ntripSourcetable

    async def requestNtripStream(self, casterUrl: str, mountPoint: str,
                                 user: str = None, passwd: str = None):
        await self.openNtripConnection(casterUrl)
        self.ntripMountPoint = mountPoint
        print(f'{time():.6f}: Connection open. Ready to write.')
        self.setRequestStreamHeader(self.casterUrl.geturl(),
                                    self.ntripMountPoint, user, passwd)
        self.ntripWriter.write(self.ntripRequestHeader)
        await self.ntripWriter.drain()
        print(f'{time():.6f}: Header sent.')
        await self.getNtripResponceHeader()
        if self.ntripResponseStatusCode == '200':
            if 'Transfer-Encoding: chunked' in self.ntripResponceHeader:
                print('Stream is chunked')
                self.ntripStreamChunked = True
            self.rtcmFramePreample = False
            self.rtcmFrameAligned = False
        else:
            print(f'Error! {self.ntripResponseStatusCode}')
            for line in self.ntripResponceHeader:
                print(line)
            self.ntripWriter.close()

    async def getRtcmFrame(self):
        rtcm3FramePreample = Bits(bin='0b11010011')
        rtcm3FrameHeaderFormat = 'bin:8, pad:6, uint:10'
        rtcmFrameComplete = False
        while not rtcmFrameComplete:
            if self.ntripStreamChunked:
                rawLine = await self.ntripReader.readuntil(b'\r\n')
                length = int(rawLine[:-2].decode('ISO-8859-1'), 16)
            rawLine = await self.ntripReader.readuntil(b'\r\n')
            receivedBytes = BitStream(rawLine[:-2])
            if self.ntripStreamChunked \
                    and receivedBytes.length != length * 8:
                print('Chunk incomplete.\n Closing connection!')
                print(f'{time():.6f}: '
                      f'Chunk {receivedBytes.length}:{length * 8}')
                break

            self.rtcmFrameBuffer += receivedBytes
            if not self.rtcmFrameAligned:
                rtcmFramePos = self.rtcmFrameBuffer.find(
                    rtcm3FramePreample, bytealigned=True)
                if rtcmFramePos:
                    self.rtcmFrameBuffer \
                        = self.rtcmFrameBuffer[rtcmFramePos[0]:]
                    self.rtcmFramePreample = True
                else:
                    self.rtcmFrameBuffer = BitStream()
            if self.rtcmFramePreample and self.rtcmFrameBuffer.length >= 48:
                (rtcmPreAmple, rtcmPayloadLength) \
                    = self.rtcmFrameBuffer.peeklist(rtcm3FrameHeaderFormat)
                rtcmFrameLength = (rtcmPayloadLength + 6) * 8
                if self.rtcmFrameBuffer.length >= rtcmFrameLength:
                    rtcmFrame = self.rtcmFrameBuffer[:rtcmFrameLength]
                    calcCrc = crc24q(rtcmFrame[:-24])
                    frameCrc = rtcmFrame[-24:].unpack('uint:24')
                    if calcCrc == frameCrc[0]:
                        self.rtcmFrameAligned = True
                        self.rtcmFrameBuffer \
                            = self.rtcmFrameBuffer[rtcmFrameLength:]
                        rtcmFrameComplete = True
                    else:
                        self.rtcmFrameAligned = False
                        self.rtcmFrameBuffer = self.rtcmFrameBuffer[8:]
                        print('!!! Warning CRC mismatch realigning!!!')
                        print(f'{time():.6f}: ' +
                              f'CRC: {hex(calcCrc)} {rtcmFrame[-24:]}')
        return rtcmFrame, time()
