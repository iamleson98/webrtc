// One call participant in a browser frame, driven by the LMS web app's own CallClient. The page
// that hosts the frames (served by compat/interop in -browser mode) calls window.api.
import { CallClient } from 'LMS_CALL_CLIENT';
import { realtime } from './realtime-stub';

type Role = 'teacher' | 'speaker' | 'viewer' | 'viewer-low' | 'listener';

const params = new URLSearchParams(location.search);
const name = params.get('name') ?? 'anon';
const role = (params.get('role') ?? 'listener') as Role;
const call = params.get('call') ?? 'call';

// The client keeps its peer connection private; keep a handle to read its statistics.
const connections: RTCPeerConnection[] = [];
const NativePeerConnection = window.RTCPeerConnection;
window.RTCPeerConnection = class extends NativePeerConnection {
	constructor(configuration?: RTCConfiguration) {
		super(configuration);
		connections.push(this);
	}
};

class StubState {
	status = 'idle';
	error: string | null = null;
	channelId = `channel-${call}`;
	mySessionId = '';
	micOn = false;
	cameraOn = false;
	screenOn = false;
	handRaised = false;
	localCamera: MediaStream | null = null;
	screen: unknown = null;
	videoStreams = new Map<string, MediaStream>();
	quality: unknown = null;
	devices: unknown[] = [];
	problems: unknown[] = [];
	selected = { audioinput: '', videoinput: '', audiooutput: '' };
	get joined(): boolean {
		return this.status !== 'idle';
	}
	addProblem(problem: unknown): void {
		this.problems.push(problem);
	}
	dismissProblem(): void {}
	resetSession(): void {
		this.status = 'idle';
	}
}

const state = new StubState();
const client = new CallClient(state as never, { teacher: role === 'teacher' });
const ws = new WebSocket(`ws://${location.host}/ws?name=${name}&call=${call}`);
const opened = new Promise<void>((resolve) => (ws.onopen = () => resolve()));
realtime.attach((message) => ws.send(message));
ws.onmessage = (event) => {
	const { event: kind, data } = JSON.parse(event.data);
	switch (kind) {
		case 'custom_calls_join':
			client.handleJoinAck(data.connID, data.iceServers);
			break;
		case 'custom_calls_signal':
			void client.signal(data.data);
			break;
		case 'custom_calls_video_demand':
			client.setVideoDemand(data.demand);
			break;
	}
};

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function until(what: string, ms: number, done: () => boolean): Promise<void> {
	const end = performance.now() + ms;
	while (performance.now() < end) {
		if (done()) return;
		await sleep(50);
	}
	throw new Error(`${name}: ${what} (status ${state.status}, error ${state.error})`);
}

(window as never as { api: unknown }).api = {
	name,
	role,
	/** Joins and resolves with the milliseconds it took to be connected. */
	async join(): Promise<number> {
		await opened;
		const start = performance.now();
		await client.join({ mic: role !== 'listener', camera: role === 'teacher' || role === 'speaker' });
		await until('never connected', 30_000, () => state.status === 'connected');
		return performance.now() - start;
	},
	watch(teacherSession: string | null, small: boolean): void {
		client.setWantedVideo(teacherSession ? [teacherSession] : [], small ? [teacherSession!] : []);
	},
	setCamera: (on: boolean) => client.setCamera(on),
	setMic: (on: boolean) => client.setMic(on),
	leave(): void {
		client.leave();
		ws.close();
	},
	async report() {
		const out = {
			name,
			role,
			status: state.status,
			error: state.error,
			connection: connections[0]?.connectionState ?? 'none',
			cameras: [...state.videoStreams.keys()],
			audioIn: 0,
			audioBytesIn: 0,
			videoIn: [] as unknown[],
			videoOut: [] as unknown[],
		};
		const pc = connections[0];
		if (!pc) return out;
		(await pc.getStats()).forEach((s: any) => {
			if (s.type === 'inbound-rtp' && s.kind === 'audio') {
				out.audioIn += s.packetsReceived ?? 0;
				out.audioBytesIn += s.bytesReceived ?? 0;
			} else if (s.type === 'inbound-rtp' && s.kind === 'video') {
				out.videoIn.push({
					packets: s.packetsReceived ?? 0,
					frames: s.framesDecoded ?? 0,
					width: s.frameWidth ?? 0,
					height: s.frameHeight ?? 0,
					lost: s.packetsLost ?? 0,
				});
			} else if (s.type === 'outbound-rtp' && s.kind === 'video') {
				out.videoOut.push({
					rid: s.rid ?? '',
					packets: s.packetsSent ?? 0,
					frames: s.framesEncoded ?? 0,
					width: s.frameWidth ?? 0,
				});
			}
		});
		return out;
	},
};
