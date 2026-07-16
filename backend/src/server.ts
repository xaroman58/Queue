import http from 'http';
import { WebSocketServer, WebSocket } from 'ws';
import { URL } from 'url';

const PORT = process.env.PORT || 3000;
const TIME_FOR_LEADER = 20;

const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('Kolejka backend zabezpieczony i dziala!');
});

const wss = new WebSocketServer({ server });

// Twardy limit połączeń na jeden adres IP (ochrona przed botnetami)
const MAX_CONNECTIONS_PER_IP = 3;
const ipConnections = new Map<string, number>();

interface Player {
    id: string;
    ws: WebSocket;
    username: string;
    lastMessageTime: number; // Do Rate Limitingu
}

let queue: Player[] = [];
let leaderTimer: NodeJS.Timeout | null = null;
let timeLeftForLeader = TIME_FOR_LEADER;

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
                timeLeft: timeLeftForLeader
            }));
        }
    });
};

const startLeaderTimer = () => {
    if (leaderTimer) { clearInterval(leaderTimer); leaderTimer = null; }
    if (queue.length === 0) return;

    timeLeftForLeader = TIME_FOR_LEADER;
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
        currentLeader.ws.send(JSON.stringify({ type: 'KICKED', reason: 'Twój czas minął! Wracasz na koniec.' }));
    }

    queue.shift();
    queue.push(currentLeader);

    broadcast({ type: 'CLEAR_BANNER' });
    broadcast({ type: 'CHAT_MSG', system: true, text: `Czas użytkownika ${currentLeader.username} minął.` });

    startLeaderTimer();
};

wss.on('connection', (ws: WebSocket, req: http.IncomingMessage) => {
    // 1. WERYFIKACJA IP I LIMITOWANIE POŁĄCZEŃ
    const ip = req.socket.remoteAddress || 'unknown';
    const currentConnections = ipConnections.get(ip) || 0;
    
    if (currentConnections >= MAX_CONNECTIONS_PER_IP) {
        console.log(`[ZABEZPIECZENIE] Odrzucono połączenie z IP: ${ip} (Limit wyczerpany)`);
        ws.close(1008, 'Zbyt wiele połączeń z tego adresu IP.');
        return;
    }
    ipConnections.set(ip, currentConnections + 1);

    // 2. POBRANIE NAZWY UŻYTKOWNIKA Z PARAMETRÓW URL (od Google)
    const requestUrl = new URL(req.url || '/', `http://${req.headers.host}`);
    const nameFromUrl = requestUrl.searchParams.get('name');
    
    const playerId = Math.random().toString(36).substring(2, 9);
    // Zabezpieczenie długości nicku z zewnątrz (max 20 znaków)
    const username = nameFromUrl ? nameFromUrl.substring(0, 20) : `Gracz_${playerId}`;
    
    const newPlayer: Player = { 
        id: playerId, 
        ws, 
        username,
        lastMessageTime: 0 
    };
    
    queue.push(newPlayer);
    
    ws.send(JSON.stringify({ type: 'WELCOME', id: playerId, username }));

    if (queue.length === 1) startLeaderTimer();
    else broadcastQueueState();

    broadcast({ type: 'CHAT_MSG', system: true, text: `${username} joined the queue.` });

    ws.on('message', (messageBuffer) => {
        try {
            const data = JSON.parse(messageBuffer.toString());

            if (data.type === 'CHAT_MSG') {
                const senderIndex = queue.findIndex(p => p.id === playerId);
                if (senderIndex === -1) return;

                const sender = queue[senderIndex];
                const now = Date.now();

                // 3. RATE LIMITING (Max 1 wiadomość na 500ms)
                if (now - sender.lastMessageTime < 500) {
                    return; // Cichy drop pakietu (ochrona przed spamem)
                }
                sender.lastMessageTime = now;

                const isFirst = (senderIndex === 0);
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
        } catch (err) {
            console.error('Błąd parsowania:', err);
        }
    });

    ws.on('close', () => {
        // Zwalnianie limitu IP po wyjściu gracza
        const connections = ipConnections.get(ip) || 1;
        ipConnections.set(ip, connections - 1);

        const wasLeader = queue[0]?.id === playerId;
        queue = queue.filter(p => p.id !== playerId);

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
    console.log(`[SERWER] Zabezpieczony silnik nasłuchuje na porcie ${PORT}`); 
});