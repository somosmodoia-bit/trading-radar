import express from 'express';
import http from 'http';
import { WebSocketServer, WebSocket } from 'ws';

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

app.use(express.static('public'));

app.get('/health', (req, res) => {
  res.status(200).send('OK');
});

// Buffer de noticias y alertas para nuevos clientes
const NEWS_HISTORY = [
  { source: "TREE NEWS", title: "US CPI MoM Actual 0.2% (Forecast 0.2%, Previous 0.2%)" },
  { source: "BLOOMBERG", title: "Federal Reserve leaves benchmark rate unchanged at 5.25%-5.50%" },
  { source: "TREE NEWS", title: "BlackRock Bitcoin ETF records +$318M net inflow in single session" },
  { source: "REUTERS", title: "SEC closes inquiry into Ethereum Foundation without enforcement action" },
  { source: "TREE NEWS", title: "Whale Alert: 4,500 BTC ($370M) transferred from Coinbase to unknown wallet" }
];

const ALERTS_HISTORY = [];

const NEWS_MOCK = [
  { source: "TREE NEWS", title: "Whale Alert: 2,100 BTC transferred to Binance Futures deposit" },
  { source: "BLOOMBERG", title: "US Treasury 10Y Yield holds steady ahead of FOMC remarks" },
  { source: "TREE NEWS", title: "MicroStrategy acquires additional 5,200 BTC at average $81,400" },
  { source: "REUTERS", title: "Global central banks expand liquidity swap lines" },
  { source: "COINDESK", title: "Perpetual funding rate neutral across top tier exchanges" }
];

let newsIndex = 0;
setInterval(() => {
  const item = NEWS_MOCK[newsIndex % NEWS_MOCK.length];
  newsIndex++;
  NEWS_HISTORY.unshift(item);
  if (NEWS_HISTORY.length > 20) NEWS_HISTORY.pop();

  const msg = JSON.stringify({ type: 'NEWS_TAPE', item });
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(msg);
    }
  });
}, 12000);

// Configuración de Detección Equilibrada para Demo Institucional
let binanceWs;
const SPOOF_THRESHOLD_BTC = 8.0;        // Umbral óptimo: 8 BTC (~$650.000 USD)
const MIN_SPOOF_DURATION_SEC = 0.8;     // Descartar parpadeos de red menores a 800ms
const MAX_SPOOF_DURATION_SEC = 4.2;     // Ventana táctica de spoofing
const orderHistory = new Map();
const alertCooldowns = new Map();       // Evita spam del mismo nivel

const SERVER_START_TIME = Date.now();
const WARMUP_MS = 10000;                // 10s de espera inicial al arrancar

function connectBinance() {
  try {
    binanceWs = new WebSocket('wss://fstream.binance.com/ws/btcusdt@depth20@100ms');

    binanceWs.on('message', (raw) => {
      try {
        const data = JSON.parse(raw.toString());
        if (!data.b || !data.a) return;

        const bids = data.b.map(([p, q]) => [parseFloat(p), parseFloat(q)]);
        const asks = data.a.map(([p, q]) => [parseFloat(p), parseFloat(q)]);

        const bestBid = bids[0] ? bids[0][0] : 0;
        const bestAsk = asks[0] ? asks[0][0] : 0;
        const midPrice = bestBid && bestAsk ? ((bestBid + bestAsk) / 2).toFixed(2) : '0.00';
        const spread = bestBid && bestAsk ? (bestAsk - bestBid).toFixed(2) : '0.00';

        const now = Date.now();
        const currentActiveKeys = new Set();

        // Rastreamos los primeros 10 niveles del libro
        const coreBids = bids.slice(0, 10);
        const coreAsks = asks.slice(0, 10);

        coreBids.forEach(([price, qty]) => {
          const key = `bid_${price.toFixed(2)}`;
          currentActiveKeys.add(key);
          if (!orderHistory.has(key)) {
            orderHistory.set(key, { price, qty, side: 'bid', firstSeen: now });
          } else {
            orderHistory.get(key).qty = qty;
          }
        });

        coreAsks.forEach(([price, qty]) => {
          const key = `ask_${price.toFixed(2)}`;
          currentActiveKeys.add(key);
          if (!orderHistory.has(key)) {
            orderHistory.set(key, { price, qty, side: 'ask', firstSeen: now });
          } else {
            orderHistory.get(key).qty = qty;
          }
        });

        // Evaluación algorítmica de cancelaciones institucionales
        if (now - SERVER_START_TIME > WARMUP_MS) {
          for (const [key, record] of orderHistory.entries()) {
            if (!currentActiveKeys.has(key)) {
              const duration = (now - record.firstSeen) / 1000;
              const lastAlerted = alertCooldowns.get(key) || 0;

              // Condición: > 8 BTC, entre 0.8s y 4.2s de vida, y sin alerta reciente en ese precio (30s cooldown)
              if (record.qty >= SPOOF_THRESHOLD_BTC && 
                  duration >= MIN_SPOOF_DURATION_SEC && 
                  duration <= MAX_SPOOF_DURATION_SEC &&
                  (now - lastAlerted > 30000)) {

                // Verificar que no haya sido ejecutada por el mercado
                const wasExecuted = (record.side === 'ask' && record.price <= bestBid) ||
                                    (record.side === 'bid' && record.price >= bestAsk);

                if (!wasExecuted) {
                  alertCooldowns.set(key, now);

                  const alerta = {
                    side: record.side,
                    price: record.price.toFixed(2),
                    volumenBTC: record.qty.toFixed(2),
                    volumenUSD: `$${(record.qty * record.price).toLocaleString('en-US', { maximumFractionDigits: 0 })}`,
                    duracionSeg: duration.toFixed(1),
                    severidad: record.qty >= 15 ? 'CRÍTICO' : 'ALTO'
                  };

                  ALERTS_HISTORY.unshift(alerta);
                  if (ALERTS_HISTORY.length > 25) ALERTS_HISTORY.pop();

                  const alertPayload = JSON.stringify({ type: 'ALERT_SPOOF', alerta });
                  wss.clients.forEach(c => {
                    if (c.readyState === WebSocket.OPEN) c.send(alertPayload);
                  });
                }
              }
              orderHistory.delete(key);
            } else if (now - record.firstSeen > 12000) {
              orderHistory.delete(key);
            }
          }
        }

        // Emitir L2 a terminales
        const l2Payload = JSON.stringify({
          type: 'ORDER_BOOK_UPDATE',
          bids: bids.slice(0, 10),
          asks: asks.slice(0, 10),
          midPrice,
          spread
        });

        wss.clients.forEach(client => {
          if (client.readyState === WebSocket.OPEN) {
            client.send(l2Payload);
          }
        });
      } catch (err) {
        console.error('Error parseando Binance:', err.message);
      }
    });

    binanceWs.on('close', () => setTimeout(connectBinance, 2000));
    binanceWs.on('error', () => { try { binanceWs.close(); } catch(e){} });
  } catch (e) {
    console.error('Error conectando Binance:', e.message);
    setTimeout(connectBinance, 3000);
  }
}

connectBinance();

wss.on('connection', (ws) => {
  try {
    if (NEWS_HISTORY.length > 0) {
      ws.send(JSON.stringify({ type: 'INIT_NEWS', items: NEWS_HISTORY }));
    }
    if (ALERTS_HISTORY.length > 0) {
      ws.send(JSON.stringify({ type: 'INIT_ALERTS', items: ALERTS_HISTORY }));
    }
  } catch (err) {
    console.error('Error enviando init:', err.message);
  }
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`[Trading Terminal] Operando en puerto ${PORT}`);
});
