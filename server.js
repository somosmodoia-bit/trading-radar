import express from 'express';
import http from 'http';
import { WebSocketServer, WebSocket } from 'ws';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const PORT = process.env.PORT || 10000;

app.use(express.static(path.join(__dirname, 'public')));
app.get('/health', (req, res) => res.status(200).send('OK'));

// Estado en memoria
const recentAlerts = [];
const orderMemory = new Map(); // price_side -> { qty, timestamp, side, price }

// Broadcast a los clientes conectados
function broadcast(data) {
  const payload = JSON.stringify(data);
  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(payload);
    }
  });
}

// ─────────────────────────────────────────────────────────────
// MOTOR DE DETECCIÓN: SPOOFING & TRAMPAS DE LIQUIDEZ
// ─────────────────────────────────────────────────────────────
const THRESHOLD_BTC_SPOOF = 8.0;     // Mínimo de BTC para considerarlo orden institucional
const MAX_SPOOF_LIFETIME_MS = 4500;  // Si desaparece en menos de 4.5 seg sin tradear -> Spoofing

function procesarDeteccion(side, price, qty) {
  const key = `${side}_${price}`;
  const now = Date.now();

  if (qty > 0) {
    if (qty >= THRESHOLD_BTC_SPOOF) {
      if (!orderMemory.has(key)) {
        orderMemory.set(key, { qty, timestamp: now, side, price });
      }
    }
  } else {
    // La orden fue retirada (volumen cayó a 0)
    if (orderMemory.has(key)) {
      const prev = orderMemory.get(key);
      const lifetime = now - prev.timestamp;

      // Si duró menos de 4.5 segundos -> Alerta de muro falso (Spoofing)
      if (lifetime <= MAX_SPOOF_LIFETIME_MS) {
        const alerta = {
          id: Date.now() + Math.random(),
          timestamp: new Date().toLocaleTimeString('es-AR', { hour12: false }),
          tipo: side === 'bid' ? 'SPOOFING COMPRA (Fake Wall)' : 'SPOOFING VENTA (Fake Wall)',
          side,
          price: prev.price.toFixed(2),
          volumenBTC: prev.qty.toFixed(3),
          volumenUSD: (prev.qty * prev.price).toLocaleString('en-US', { style: 'currency', currency: 'USD' }),
          duracionSeg: (lifetime / 1000).toFixed(1),
          severidad: prev.qty >= 15 ? 'CRÍTICO' : 'ALTO'
        };

        recentAlerts.unshift(alerta);
        if (recentAlerts.length > 30) recentAlerts.pop();

        broadcast({ type: 'ALERT_SPOOF', alerta });
      }
      orderMemory.delete(key);
    }
  }
}

// ─────────────────────────────────────────────────────────────
// CONEXIÓN DIRECTA A BINANCE FUTURES (BTCUSDT @depth20@100ms)
// ─────────────────────────────────────────────────────────────
let binanceWS = null;

function conectarBinance() {
  const symbol = 'btcusdt';
  const url = `wss://fstream.binance.com/ws/${symbol}@depth20@100ms`;

  binanceWS = new WebSocket(url);

  binanceWS.on('open', () => {
    console.log('[Binance Futures] Conexión activa a libro L2.');
  });

  binanceWS.on('message', (raw) => {
    try {
      const data = JSON.parse(raw);
      if (!data.b || !data.a) return;

      const bids = data.b.map(([p, q]) => [parseFloat(p), parseFloat(q)]);
      const asks = data.a.map(([p, q]) => [parseFloat(p), parseFloat(q)]);

      bids.forEach(([p, q]) => procesarDeteccion('bid', p, q));
      asks.forEach(([p, q]) => procesarDeteccion('ask', p, q));

      const midPrice = ((bids[0][0] + asks[0][0]) / 2).toFixed(2);
      const spread = (asks[0][0] - bids[0][0]).toFixed(2);

      broadcast({
        type: 'ORDER_BOOK_UPDATE',
        bids: bids.slice(0, 10),
        asks: asks.slice(0, 10),
        midPrice,
        spread
      });
    } catch (err) {}
  });

  binanceWS.on('close', () => {
    setTimeout(conectarBinance, 2500);
  });

  binanceWS.on('error', () => {
    try { binanceWS.close(); } catch(e) {}
  });
}

conectarBinance();

// ─────────────────────────────────────────────────────────────
// FEED DE NOTICIAS DE ALTO IMPACTO (Algorítmico)
// ─────────────────────────────────────────────────────────────
const MOCK_NEWS = [
  { source: 'TREE_NEWS', title: 'FED: ANUNCIA PAUSA EN POLÍTICA MONETARIA Y EVALÚA LIQUIDEZ', impact: 'ALTO' },
  { source: 'BLOOMBERG', title: 'DATOS DE INFLACIÓN SUBYACENTE EN EE. UU. DESCIENDEN 0.3%', impact: 'MEDIO' },
  { source: 'TREE_NEWS', title: 'ALERTA ON-CHAIN: 4.200 BTC RETIRADOS DE COLD WALLETS A DERIVADOS', impact: 'ALTO' },
  { source: 'REUTERS', title: 'COMISIÓN DE VALORES APRUEBA NUEVAS LICENCIAS DE CUSTODIA SPOT', impact: 'MEDIO' },
  { source: 'COINDESK', title: 'FONDOS INSTITUCIONALES REPORTAN ENTRADA NETA RÉCORD ESTA SEMANA', impact: 'ALTO' }
];

setInterval(() => {
  const item = MOCK_NEWS[Math.floor(Math.random() * MOCK_NEWS.length)];
  broadcast({
    type: 'NEWS_TAPE',
    item: {
      ...item,
      id: Date.now(),
      time: new Date().toLocaleTimeString('es-AR', { hour12: false })
    }
  });
}, 15000);

wss.on('connection', (ws) => {
  ws.send(JSON.stringify({ type: 'INIT_ALERTS', alerts: recentAlerts }));
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`[Trading Terminal] Operando en puerto ${PORT}`);
});
