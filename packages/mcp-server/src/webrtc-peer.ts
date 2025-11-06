import { RTCPeerConnection, RTCSessionDescription } from 'werift';
import { Logger } from './logger.js';
import type { Config } from './config.js';

export interface WebRTCPeerOptions {
  config: Config['foundry']['webrtc'];
  logger: Logger;
  onMessage: (message: any) => Promise<void>;
}

/**
 * WebRTC peer connection for Node.js server
 * Handles WebRTC signaling and data channel communication
 */
export class WebRTCPeer {
  private peerConnection: RTCPeerConnection | null = null;
  private dataChannel: any = null;
  private logger: Logger;
  private config: Config['foundry']['webrtc'];
  private onMessageHandler: (message: any) => Promise<void>;
  private isConnected = false;
  private pendingChunks: Map<string, { chunks: Map<number, string>, totalChunks: number, originalType: string, originalId: string }> = new Map();

  constructor({ config, logger, onMessage }: WebRTCPeerOptions) {
    this.config = config;
    this.logger = logger.child({ component: 'WebRTCPeer' });
    this.onMessageHandler = onMessage;
  }

  /**
   * Handle incoming WebRTC offer from browser client
   * Returns answer to be sent back to client
   *
   * Critical: Send answer IMMEDIATELY, then trickle ICE candidates
   * Don't wait for data channel or ICE gathering before answering
   */
  async handleOffer(offer: RTCSessionDescriptionInit): Promise<RTCSessionDescriptionInit> {
    const startTime = Date.now();
    this.logger.info('[WebRTC Timing] Received offer from client');

    // Create peer connection WITHOUT STUN servers for localhost connections
    this.peerConnection = new RTCPeerConnection({
      iceServers: [] // Empty for localhost - no external STUN needed
    });

    this.setupPeerConnectionHandlers();

    // Step 1: Set remote description (offer from client)
    const t1 = Date.now();
    await this.peerConnection.setRemoteDescription(offer as any);
    this.logger.info(`[WebRTC Timing] setRemoteDescription took ${Date.now() - t1}ms`);

    // Step 2: Create answer IMMEDIATELY - don't wait for data channel or ICE
    const t2 = Date.now();
    const answer = await this.peerConnection.createAnswer();
    this.logger.info(`[WebRTC Timing] createAnswer took ${Date.now() - t2}ms`);

    // Step 3: Set local description
    const t3 = Date.now();
    await this.peerConnection.setLocalDescription(answer);
    this.logger.info(`[WebRTC Timing] setLocalDescription took ${Date.now() - t3}ms`);

    this.logger.info(`[WebRTC Timing] Answer ready in ${Date.now() - startTime}ms - sending immediately`);

    // Data channel and ICE will arrive later via events - don't wait!
    // The ondatachannel event will fire when the channel is ready

    return this.peerConnection.localDescription as RTCSessionDescriptionInit;
  }

  private setupPeerConnectionHandlers(): void {
    if (!this.peerConnection) return;

    // ICE gathering state changes
    this.peerConnection.iceGatheringStateChange.subscribe((state) => {
      this.logger.info(`[WebRTC] ICE gathering state: ${state}`);
    });

    // ICE connection state changes
    this.peerConnection.iceConnectionStateChange.subscribe((state) => {
      this.logger.info(`[WebRTC] ICE connection state: ${state}`);

      if (state === 'failed') {
        this.logger.error('[WebRTC] ICE connection failed - check STUN/TURN config or firewall');
        this.isConnected = false;
      } else if (state === 'disconnected' || state === 'closed') {
        this.isConnected = false;
      } else if (state === 'connected') {
        this.logger.info('[WebRTC] ICE connection established');
      }
    });

    // Overall peer connection state
    this.peerConnection.onconnectionstatechange = () => {
      const state = this.peerConnection?.connectionState;
      this.logger.info(`[WebRTC] Peer connection state: ${state}`);

      if (state === 'connected') {
        this.logger.info('[WebRTC] Peer connection fully established');
        this.isConnected = true;
      } else if (state === 'failed') {
        this.logger.error('[WebRTC] Peer connection failed - DTLS handshake may have failed');
        this.isConnected = false;
      } else if (state === 'disconnected' || state === 'closed') {
        this.isConnected = false;
      }
    };

    // Data channel from client (critical event!)
    this.peerConnection.ondatachannel = (event: any) => {
      this.logger.info('[WebRTC] Data channel received from client!');
      this.dataChannel = event.channel;
      this.setupDataChannelHandlers();
    };
  }

  private setupDataChannelHandlers(): void {
    if (!this.dataChannel) return;

    this.dataChannel.onopen = () => {
      this.logger.info('[WebRTC] ✓ Data channel opened - connection fully ready!');
      this.isConnected = true;
    };

    this.dataChannel.onclose = () => {
      this.logger.info('[WebRTC] Data channel closed');
      this.isConnected = false;
    };

    this.dataChannel.onerror = (error: any) => {
      this.logger.error('[WebRTC] Data channel error:', error);
    };

    this.dataChannel.onmessage = async (event: any) => {
      try {
        console.error('[WebRTC DEBUG] Data channel received raw message', {
          dataLength: event.data?.length,
          dataPreview: event.data?.substring(0, 100)
        });
        const message = JSON.parse(event.data);

        // Handle chunked messages
        if (message.type === 'chunked-message') {
          await this.handleChunkedMessage(message);
          return;
        }

        console.error('[WebRTC DEBUG] Parsed message successfully', {
          type: message.type,
          requestId: message.requestId,
          hasData: !!message.data
        });
        await this.onMessageHandler(message);
        console.error('[WebRTC DEBUG] Message handler completed', { type: message.type });
      } catch (error) {
        console.error('[WebRTC DEBUG] Failed to parse or handle message', {
          error: error instanceof Error ? error.message : String(error),
          rawData: event.data?.substring(0, 200)
        });
      }
    };
  }


  sendMessage(message: any): void {
    if (!this.dataChannel || !this.isConnected) {
      this.logger.warn('Cannot send message - data channel not open');
      return;
    }

    try {
      this.dataChannel.send(JSON.stringify(message));
      this.logger.debug('Sent WebRTC message', { type: message.type });
    } catch (error) {
      this.logger.error('Failed to send WebRTC message', error);
    }
  }

  private async handleChunkedMessage(chunkMessage: any): Promise<void> {
    const { chunkId, chunkIndex, totalChunks, chunk, originalType, originalId } = chunkMessage;

    console.error(`[WebRTC DEBUG] Received chunk ${chunkIndex + 1}/${totalChunks} for ${originalType}`);

    // Initialize chunk storage for this message
    if (!this.pendingChunks.has(chunkId)) {
      this.pendingChunks.set(chunkId, {
        chunks: new Map(),
        totalChunks,
        originalType,
        originalId
      });
    }

    const pending = this.pendingChunks.get(chunkId)!;
    pending.chunks.set(chunkIndex, chunk);

    console.error(`[WebRTC DEBUG] Collected ${pending.chunks.size}/${totalChunks} chunks`);

    // Check if we have all chunks
    if (pending.chunks.size === totalChunks) {
      console.error(`[WebRTC DEBUG] All chunks received, reassembling message`);

      // Reassemble in order
      let reassembled = '';
      for (let i = 0; i < totalChunks; i++) {
        reassembled += pending.chunks.get(i) || '';
      }

      console.error(`[WebRTC DEBUG] Reassembled ${reassembled.length} bytes`);

      // Parse and handle the complete message
      try {
        const completeMessage = JSON.parse(reassembled);
        console.error(`[WebRTC DEBUG] Parsed reassembled message successfully`);
        await this.onMessageHandler(completeMessage);
        console.error(`[WebRTC DEBUG] Reassembled message handler completed`);
      } catch (error) {
        console.error(`[WebRTC DEBUG] Failed to parse reassembled message:`, error);
      }

      // Clean up
      this.pendingChunks.delete(chunkId);
    }
  }

  disconnect(): void {
    if (this.dataChannel) {
      this.dataChannel.close();
      this.dataChannel = null;
    }

    if (this.peerConnection) {
      this.peerConnection.close();
      this.peerConnection = null;
    }

    this.isConnected = false;
    this.pendingChunks.clear();
    this.logger.info('WebRTC peer disconnected');
  }

  getIsConnected(): boolean {
    return this.isConnected;
  }
}
