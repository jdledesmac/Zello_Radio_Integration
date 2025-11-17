import os
import asyncio
import base64
import json
import time
import aiohttp
import socket
import configparser
#from opus import encoder, decoder
from pyogg import OpusEncoder as encoder
from pyogg import OpusDecoder as decoder
import audioop
import random
from time import time as timestamper



#PARAMETERS
WS_ENDPOINT= None
UDP_IP = 0
UDP_PORT = 0 
WS_TIMEOUT_SEC = 2
RTP_CHUNK=0
CHANNELS = 1
RATE = 0
packet_duration = 20
ZelloWS = None
ZelloStreamID = None
server_address = None

#MAIN FUNCTION
def main():
    ''' Main function: Load config and start Zello audio streaming '''
    global ZelloWS, ZelloStreamID, server_address, WS_ENDPOINT, UDP_IP, UDP_PORT, RATE, RTP_CHUNK
    try:
        #os.chdir("../")
        config = configparser.ConfigParser()
        config.read('stream.conf')
        username = config['zello']['username']
        password = config['zello']['password']
        token = config['zello']['token']
        channel = config['zello']['channel']
        serverIp=config['init']['serverIP']
        serverPort=int(config['init']['serverPort'])
        server_address = (serverIp, serverPort)
        WS_ENDPOINT = config['init']['WS_ENDPOINT']
        UDP_IP =config['init']['UDP_IP']
        UDP_PORT = int(config['init']['UDP_PORT'])
        RATE = int(config['init']['RATE'])
        RTP_CHUNK = int(config['init']['RTP_CHUNK'])
        print(WS_ENDPOINT)
        print(UDP_PORT)
        print(server_address)
    except KeyError as error:
        print("Check config file. Missing key:", error)
        return

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(zello_stream_audio_to_channel(username, password,
            token, channel))
    except KeyboardInterrupt:
        try:
            if ZelloWS and ZelloStreamID:
                loop.run_until_complete(zello_stream_stop(ZelloWS, ZelloStreamID))
        except aiohttp.client_exceptions.ClientError as error:
            print("Error during stopping. ", error)

        def shutdown_exception_handler(loop, context):
            if "exception" in context and isinstance(context["exception"], asyncio.CancelledError):
                return
            loop.default_exception_handler(context)

        loop.set_exception_handler(shutdown_exception_handler)
        tasks = asyncio.gather(*asyncio.all_tasks(loop=loop), return_exceptions=True)
        tasks.add_done_callback(lambda t: loop.stop())
        tasks.cancel()
        while not tasks.done() and not loop.is_closed():
            loop.run_forever()
        print("Stopped by user")
    finally:
        loop.close()

#FUNCTIONS FOR ZELLO STREAMING VIA WEBSOCKET AND RTP SENDER/RECEIVER
async def zello_stream_audio_to_channel(username, password, token, channel):
    # Pass out the opened WebSocket and StreamID to handle synchronous keyboard interrupt
    global ZelloWS, ZelloStreamID, server_address, WS_ENDPOINT, UDP_IP, UDP_PORT, RATE, RTP_CHUNK
    try:
        conn = aiohttp.TCPConnector(family = socket.AF_INET, ssl = False)
        async with aiohttp.ClientSession(connector = conn) as session:
            async with session.ws_connect(WS_ENDPOINT) as ws:
                ZelloWS = ws
                await asyncio.wait_for(authenticate(ws, username, password, token, channel), WS_TIMEOUT_SEC)
                print(f"User {username} has been authenticated on {channel} channel")
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.bind(server_address)
                sock.settimeout(1)
                init_time=0
                seq=0
                OPUS_CHUNK=480
                is_Zello_free=True
                command=''
                dec=decoder.Decoder(RATE, CHANNELS)
                while True:
                    try:
                        msg = await asyncio.wait_for(listen_Zello(ws), 1)
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            print(msg)
                            msg_data = json.loads(msg.data)
                            if "command" in msg_data:
                                command = msg_data["command"]
                                if command=="on_stream_start":
                                    is_Zello_free=False
                                    init_time = int(timestamper())
                                    seq=random.randint(0, 1000)
                                    opus_packet_duration=msg_data["packet_duration"]
                                    OPUS_CHUNK=int(opus_packet_duration*RATE/1000)
                                    
                                elif command=="on_stream_stop":
                                    is_Zello_free=True

                        elif msg.type==aiohttp.WSMsgType.BINARY and command == "on_stream_start":
                            #print("ZELLO PACKET to RTP SPEAKER")
                            opus_packet = msg.data[9:]
                            send_to_Rtp(opus_packet, dec, init_time, seq, sock, OPUS_CHUNK)
                            seq+=int(OPUS_CHUNK/160)
                            init_time+=opus_packet_duration

                        elif msg.type == aiohttp.WSMsgType.CLOSED:
                            break

                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            break
                    
                    except:
                        pass

                    if is_Zello_free:
                        await listen_Rtp(ws, sock)


    except (NameError, aiohttp.client_exceptions.ClientError, IOError) as error:
        print(error)
    except asyncio.TimeoutError:
        print("Communication timeout")


async def listen_Zello(ws):
    ''' Listen for messages from Zello WebSocket '''
    msg = await ws.receive()
    return msg


async def listen_Rtp(ws, sock):
    ''' Listen for RTP packets from UDP socket and send to Zello WebSocket '''
    global ZelloStreamID, RTP_CHUNK
    enc = encoder.Encoder(RATE,CHANNELS,'audio')
    R_packet_id=0
    stream_id=0
    packet_duration_sec = packet_duration / 1000
    start_ts_sec = time.time_ns() / 1000000000
    time_streaming_sec = 0

    while True:
        try:
            connection = sock.recvfrom(1024)
            incoming_rtp=connection[0]
            raw_pcm=audioop.ulaw2lin(incoming_rtp[12:], 2)
            data=enc.encode(raw_pcm, RTP_CHUNK)
            if R_packet_id==0:
                stream_id = await asyncio.wait_for(zello_stream_start(ws), WS_TIMEOUT_SEC)
                ZelloStreamID = stream_id
                print("RTP Audio to ZELLO: Started streaming, stream_id= ", stream_id)
            R_packet_id+=1
            packet = generate_zello_stream_packet(stream_id, R_packet_id, data)
            try:
                await asyncio.wait_for(send_audio_packet(ws, packet), packet_duration_sec * 0.8)
            except asyncio.TimeoutError:
                pass
            time_streaming_sec += packet_duration_sec
            time_elapsed_sec = (time.time_ns() / 1000000000) - start_ts_sec
            sleep_delay_sec = time_streaming_sec - time_elapsed_sec
            if sleep_delay_sec > 0.001:
                time.sleep(sleep_delay_sec)
        except:
            if R_packet_id>0:
                print ("RTP Audio to ZELLO: End of Stream, stream_id= ", stream_id)
                R_packet_id=0
                await asyncio.wait_for(zello_stream_stop(ws, stream_id), WS_TIMEOUT_SEC)
                break
            else:
                R_packet_id=0
                break

def send_to_Rtp(opus_packet, dec, init_time, seq, sock, OPUS_CHUNK):
    ''' Send decoded Opus packet to RTP UDP socket '''
    
    raw_pcm=dec.decode(opus_packet, OPUS_CHUNK)
    ulaw_data=audioop.lin2ulaw(raw_pcm[:2*OPUS_CHUNK],2)
    for chunk in range(160, OPUS_CHUNK+1, 160):
        rtp_packet=generate_Rtp_packet(init_time, seq, ulaw_data[(chunk-160):chunk])
        sock.sendto(rtp_packet, (UDP_IP, UDP_PORT))
        init_time+=20
        seq+=1    
        
    

def generate_Rtp_packet(init_time, seq, ulaw_data):
    ''' 
    Generate RTP packet with given parameters 
    args:
        init_time: initial timestamp
        seq: sequence number
        ulaw_data: payload data in u-law format
    returns: 
        RTP packet as bytearray
    '''
    rtp_header=bytearray(12)
    sequence_number=seq.to_bytes(2, "big")
    timestamp=int(init_time).to_bytes(4, "big")
    sscr=(1).to_bytes(4, "big")
    rtp_header[0]=0x80     #version
    rtp_header[1]=0x00      #PCMU=0 payload type
    rtp_header[2:3]= sequence_number
    rtp_header[4:7]=timestamp
    rtp_header[8:11]=sscr
    return rtp_header[:12] + ulaw_data


async def authenticate(ws, username, password, token, channel):
    ''' Authenticate user on Zello WebSocket '''
    # help: https://github.com/zelloptt/zello-channel-api/blob/master/AUTH.md
    await ws.send_str(json.dumps({
        "command": "logon",
        "seq": 1,
        "auth_token": token,
        "username": username,
        "password": password,
        "channel": channel
    }))
    is_authorized = False
    is_channel_available = False
    async for msg in ws:
        if msg.type == aiohttp.WSMsgType.TEXT:
            data = json.loads(msg.data)
            if "refresh_token" in data:
                is_authorized = True
            elif "command" in data and "status" in data and data["command"] == "on_channel_status":
                is_channel_available = data["status"] == "online"
            if is_authorized and is_channel_available:
                break

    if not is_authorized or not is_channel_available:
        raise NameError('Authentication failed')


async def zello_stream_start(ws):
    ''' Start Zello audio stream and return stream_id '''
    global RATE, packet_duration
    sample_rate = RATE
    frames_per_packet = 1
    # Sample_rate is in little endian.
    # https://github.com/zelloptt/zello-channel-api/blob/409378acd06257bcd07e3f89e4fbc885a0cc6663/sdks/js/src/classes/utils.js#L63
    codec_header = base64.b64encode(sample_rate.to_bytes(2, "little") + \
        frames_per_packet.to_bytes(1, "big") + packet_duration.to_bytes(1, "big")).decode()

    await ws.send_str(json.dumps({
        "command": "start_stream",
        "seq": 2,
        "type": "audio",
        "codec": "opus",
        "codec_header": codec_header,
        "packet_duration": packet_duration
        }))

    async for msg in ws:
        if msg.type == aiohttp.WSMsgType.TEXT:
            data = json.loads(msg.data)
            if "success" in data and "stream_id" in data and data["success"]:
                return data["stream_id"]
            elif "error" in data:
                print("Got an error:", data["error"])
                break
            else:
                # Ignore the messages we are not interested in
                continue
    raise NameError('Failed to create Zello audio stream')


async def zello_stream_stop(ws, stream_id):
    ''' Stop Zello audio stream '''
    await ws.send_str(json.dumps({
        "command": "stop_stream",
        "stream_id": stream_id
        }))


async def send_audio_packet(ws, packet):
    ''' Send audio packet to Zello WebSocket
     args:
        ws: Zello WebSocket connection
        packet: Audio packet to send
    '''
    # Once the data has been sent - listen on websocket, connection may be closed otherwise.
    await ws.send_bytes(packet)
    await ws.receive()


def generate_zello_stream_packet(stream_id, packet_id, data):
    ''' Generate Zello stream packet
    args:
        stream_id: Zello stream ID
        packet_id: Packet sequence number
        data: Audio data payload
    returns:
        Zello stream packet as bytearray'''
    # help: https://github.com/zelloptt/zello-channel-api/blob/master/API.md#stream-data
    return (1).to_bytes(1, "big") + stream_id.to_bytes(4, "big") + \
        packet_id.to_bytes(4, "big") + data




if __name__ == "__main__":
    main()
