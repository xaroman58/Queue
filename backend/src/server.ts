import http from 'http';
import { WebSocketServer, WebSocket } from 'ws';
import { URL } from 'url';

const PORT = Number(process.env.PORT) || 3000;
const TIME_FOR_LEADER = 20;

const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('Queue warp secure engine running!');
});

const wss = new WebSocketServer({ server });

const MAX_CONNECTIONS_PER_IP = 3;
const ipConnections = new Map<string, number>();

interface Player {
    id: string;
    ws: WebSocket;
    username: string;
    lastMessageTime: number;
    lastDuelTime: number; 
}

let queue: Player[] = [];
let leaderTimer: NodeJS.Timeout | null = null;
let timeLeftForLeader = TIME_FOR_LEADER;
let leaderHasSpoken = false; 

// Aktywne pojedynki: klucz to id wyzwanego, wartość to id wyzywającego i słowo
const activeDuels = new Map<string, { challengerId: string, word: string }>();
const DUEL_WORDS = ['KLAWIATURA', 'PROCESOR', 'MOTYWY', 'KARTAGRAFIKA', 'INTERFEJS', 'ARCHITEKTURA', 'ZASILACZ'];

const broadcast = (data: object) => {
    const payload = JSON.stringify(data);
    queue.forEach(player => {
        if (player.ws.readyState === WebSocket.OPEN) {
            player.ws.send(payload);
        }
    });
};

const broadcastQueueState = () => {
    queue.forEach((player, index) => {
        if (player.ws.readyState === WebSocket.OPEN) {
            player.ws.send(JSON.stringify({
                type: 'QUEUE_UPDATE',
                position: index + 1,
                total: queue.length,
                timeLeft: timeLeftForLeader,
                isLeaderMuted: (index === 0 && leaderHasSpoken)
            }));
        }
    });
};

const startLeaderTimer = () => {
    if (leaderTimer) { clearInterval(leaderTimer); leaderTimer = null; }
    if (queue.length === 0) return;

    timeLeftForLeader = TIME_FOR_LEADER;
    leaderHasSpoken = false;
    broadcastQueueState();

    leaderTimer = setInterval(() => {
        timeLeftForLeader--;
        if (timeLeftForLeader <= 0) {
            clearInterval(leaderTimer!);
            leaderTimer = null;
            handleLeaderTimeout();
        } else {
            broadcastQueueState();
        }
    }, 1000);
};

const handleLeaderTimeout = () => {
    if (queue.length === 0) return;
    const currentLeader = queue[0];

    if (currentLeader.ws.readyState === WebSocket.OPEN) {
        currentLeader.ws.send(JSON.stringify({ type: 'KICKED', reason: 'Time is up! Back to the end.' }));
    }

    queue.shift();
    queue.push(currentLeader);
    leaderHasSpoken = false;

    broadcast({ type: 'CLEAR_BANNER' });
    broadcast({ type: 'CHAT_MSG', system: true, text: `Leader ${currentLeader.username} timed out.` });

    startLeaderTimer();
};

const swapPlayers = (indexA: number, indexB: number) => {
    const temp = queue[indexA];
    queue[indexA] = queue[indexB];
    queue[indexB] = temp;
    broadcastQueueState();
};

wss.on('connection', (ws: WebSocket, req: http.IncomingMessage) => {
    const ip = req.socket.remoteAddress || 'unknown';
    
    // --- POPRAWKA BŁĘDU CONNECTING (Zmieniona kolejność) ---
    // Pobierz najpierw nazwę użytkownika, żeby system w ogóle go zobaczył
    const requestUrl = new URL(req.url || '/', `http://${req.headers.host}`);
    const nameFromUrl = requestUrl.searchParams.get('name');
    
    const playerId = Math.random().toString(36).substring(2, 9);
    const username = nameFromUrl ? nameFromUrl.substring(0, 20) : `Player_${playerId}`;

    // Dopiero TERAZ sprawdzamy i zwiększamy limit IP
    const currentConnections = ipConnections.get(ip) || 0;
    
    if (currentConnections >= MAX_CONNECTIONS_PER_IP) {
        ws.close(1008, 'Too many connections from this IP.');
        return;
    }
    ipConnections.set(ip, currentConnections + 1);
    // -----------------------------------------------------

    const newPlayer: Player = { 
        id: playerId, 
        ws, 
        username,
        lastMessageTime: 0,
        lastDuelTime: 0
    };
    
    queue.push(newPlayer);
    
    ws.send(JSON.stringify({ type: 'WELCOME', id: playerId, username }));

    if (queue.length === 1) startLeaderTimer();
    else broadcastQueueState();

    broadcast({ type: 'CHAT_MSG', system: true, text: `${username} joined the queue.` });

    ws.on('message', (messageBuffer) => {
        try {
            const data = JSON.parse(messageBuffer.toString());
            const senderIndex = queue.findIndex(p => p.id === playerId);
            if (senderIndex === -1) return;
            const sender = queue[senderIndex];
            const now = Date.now();

            // --- STANDARD CHAT ---
            if (data.type === 'CHAT_MSG') {
                if (now - sender.lastMessageTime < 500) return; 
                sender.lastMessageTime = now;

                const isFirst = (senderIndex === 0);
                
                // Leader can speak only ONCE
                if (isFirst) {
                    if (leaderHasSpoken) return;
                    leaderHasSpoken = true;
                    broadcastQueueState(); // Refresh UI to lock input
                }

                const maxLength = isFirst ? 60 : 120;
                let safeText = data.text;
                if (safeText.length > maxLength) safeText = safeText.substring(0, maxLength);

                broadcast({
                    type: 'CHAT_MSG',
                    system: false,
                    sender: sender.username,
                    text: safeText,
                    isGlobal: isFirst,
                    position: senderIndex + 1
                });
            }

            // --- GIVE PLACE ---
            if (data.type === 'GIVE_PLACE') {
                if (senderIndex < queue.length - 1) {
                    swapPlayers(senderIndex, senderIndex + 1);
                    broadcast({ type: 'CHAT_MSG', system: true, text: `${sender.username} gave their place to the player behind.` });
                }
            }

            // --- 1v1 CHALLENGE (TYPERACER) ---
            if (data.type === 'DUEL_CHALLENGE') {
                if (now - sender.lastDuelTime < 10000) {
                    ws.send(JSON.stringify({ type: 'CHAT_MSG', system: true, text: `Wait 10 seconds before next challenge.` }));
                    return;
                }
                
                // Can challenge only the person directly in front (except leader)
                if (senderIndex > 1) { 
                    const targetIndex = senderIndex - 1;
                    const targetPlayer = queue[targetIndex];
                    
                    sender.lastDuelTime = now;
                    const randomWord = DUEL_WORDS[Math.floor(Math.random() * DUEL_WORDS.length)];
                    activeDuels.set(targetPlayer.id, { challengerId: sender.id, word: randomWord });

                    // Send start event to both
                    const duelPayload = JSON.stringify({ type: 'DUEL_START', word: randomWord, opponent: targetPlayer.username });
                    ws.send(duelPayload);
                    targetPlayer.ws.send(JSON.stringify({ type: 'DUEL_START', word: randomWord, opponent: sender.username }));
                }
            }

            // --- DUEL ANSWER ---
            if (data.type === 'DUEL_ANSWER') {
                for (const [targetId, duelData] of activeDuels.entries()) {
                    if (targetId === sender.id || duelData.challengerId === sender.id) {
                        if (data.answer.toUpperCase() === duelData.word) {
                            activeDuels.delete(targetId);
                            
                            const p1Index = queue.findIndex(p => p.id === targetId);
                            const p2Index = queue.findIndex(p => p.id === duelData.challengerId);
                            
                            if (p1Index !== -1 && p2Index !== -1) {
                                const winnerIndex = Math.min(p1Index, p2Index);
                                const loserIndex = Math.max(p1Index, p2Index);
                                
                                queue[winnerIndex] = sender;
                                queue[loserIndex] = (sender.id === targetId) ? queue[p2Index] : queue[p1Index];
                                
                                broadcastQueueState();
                                broadcast({ type: 'CHAT_MSG', system: true, text: `⚔️ ${sender.username} wins the duel and takes a better position!` });
                                
                                // Reset UI duel
                                queue[winnerIndex].ws.send(JSON.stringify({ type: 'DUEL_END' }));
                                queue[loserIndex].ws.send(JSON.stringify({ type: 'DUEL_END' }));
                            }
                        }
                        break;
                    }
                }
            }

        } catch (err) {
            console.error('Parse error:', err);
        }
    });

    ws.on('close', () => {
        const connections = ipConnections.get(ip) || 1;
        ipConnections.set(ip, connections - 1);

        const wasLeader = queue[0]?.id === playerId;
        queue = queue.filter(p => p.id !== playerId);
        activeDuels.delete(playerId);

        broadcast({ type: 'CHAT_MSG', system: true, text: `${username} opuścił kolejkę.` });

        if (wasLeader) {
            broadcast({ type: 'CLEAR_BANNER' });
            startLeaderTimer();
        } else {
            broadcastQueueState();
        }
    });
});

server.listen(PORT, '0.0.0.0', () => { 
    console.log(`[SERVER] Secured warp engine nasłuchuje na porcie ${PORT}`); 
});