'use strict';

const crypto = require('node:crypto');
const http = require('node:http');
const path = require('node:path');
const express = require('express');
const { Server } = require('socket.io');

const PORT = Number.parseInt(process.env.PORT || '3000', 10);
const FRONTEND_DIR = path.resolve(__dirname, '..', 'frontend');
const DEVICE_TOKEN = (process.env.DEVICE_TOKEN || '').trim();
const MAX_FRAME_BASE64_LENGTH = Number.parseInt(
  process.env.MAX_FRAME_BASE64_LENGTH || '600000',
  10,
);
const MIN_FRAME_INTERVAL_MS = Number.parseInt(
  process.env.MIN_FRAME_INTERVAL_MS || '180',
  10,
);
const MIN_GPS_INTERVAL_MS = Number.parseInt(
  process.env.MIN_GPS_INTERVAL_MS || '500',
  10,
);
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || '')
  .split(',')
  .map((origin) => origin.trim())
  .filter(Boolean);

const ROLES = new Set(['frontend', 'raspberry', 'esp32']);
const VALID_COMMANDS = new Set(['W', 'A', 'S', 'D', 'STOP']);
const BASE64_JPEG_PATTERN = /^[A-Za-z0-9+/]+={0,2}$/;

if (!Number.isInteger(PORT) || PORT < 1 || PORT > 65535) {
  throw new Error('PORT debe ser un entero entre 1 y 65535.');
}

const app = express();
const httpServer = http.createServer(app);
let ultimaUbicacionGps = null;

app.disable('x-powered-by');
app.set('trust proxy', 1);

function originPermitido(origin, callback) {
  // Los clientes embebidos normalmente no envían la cabecera Origin.
  if (!origin || ALLOWED_ORIGINS.length === 0 || ALLOWED_ORIGINS.includes(origin)) {
    callback(null, true);
    return;
  }

  callback(new Error('Origen no permitido.'));
}

const io = new Server(httpServer, {
  cors: {
    origin: originPermitido,
    methods: ['GET', 'POST'],
  },
  transports: ['websocket', 'polling'],
  maxHttpBufferSize: Math.max(MAX_FRAME_BASE64_LENGTH + 1024, 1_000_000),
  perMessageDeflate: false,
  serveClient: true,
});

function tokenValido(tokenRecibido) {
  if (!DEVICE_TOKEN) return true;

  const esperado = Buffer.from(DEVICE_TOKEN);
  const recibido = Buffer.from(String(tokenRecibido || ''));
  return esperado.length === recibido.length && crypto.timingSafeEqual(esperado, recibido);
}

io.use((socket, next) => {
  const rol = String(socket.handshake.auth?.rol || '').toLowerCase();

  if (!ROLES.has(rol)) {
    next(new Error('Rol de cliente inválido.'));
    return;
  }

  if ((rol === 'raspberry' || rol === 'esp32') && !tokenValido(socket.handshake.auth?.token)) {
    next(new Error('Dispositivo no autorizado.'));
    return;
  }

  socket.data.rol = rol;
  next();
});

io.on('connection', (socket) => {
  const { rol } = socket.data;
  socket.join(rol);
  socket.data.ultimoFrameEn = 0;
  socket.data.ultimoGpsEn = 0;

  console.info(`[socket] conectado rol=${rol} id=${socket.id}`);

  if (rol === 'frontend' && ultimaUbicacionGps) {
    socket.emit('actualizacion_gps', ultimaUbicacionGps);
  }

  socket.on('comando_front', (payload) => {
    if (rol !== 'frontend' || typeof payload !== 'string') return;

    const comando = payload.trim().toUpperCase();
    if (!VALID_COMMANDS.has(comando)) {
      socket.emit('error_protocolo', 'Comando de movimiento inválido.');
      return;
    }

    io.to('raspberry').emit('comando_pi', comando);
  });

  socket.on('datos_gps', (payload) => {
    if (
      rol !== 'raspberry'
      || payload === null
      || typeof payload !== 'object'
      || Array.isArray(payload)
    ) {
      return;
    }

    const { latitud, longitud, fuente, satelites } = payload;
    if (
      typeof latitud !== 'number'
      || typeof longitud !== 'number'
      || !Number.isFinite(latitud)
      || !Number.isFinite(longitud)
      || latitud < -90
      || latitud > 90
      || longitud < -180
      || longitud > 180
    ) {
      return;
    }

    if (
      fuente !== undefined
      && (
        typeof fuente !== 'string'
        || fuente.trim().length === 0
        || fuente.trim().length > 40
      )
    ) {
      return;
    }

    if (
      satelites !== undefined
      && (
        !Number.isInteger(satelites)
        || satelites < 0
        || satelites > 99
      )
    ) {
      return;
    }

    const ahora = Date.now();
    if (ahora - socket.data.ultimoGpsEn < MIN_GPS_INTERVAL_MS) return;

    socket.data.ultimoGpsEn = ahora;
    ultimaUbicacionGps = {
      latitud,
      longitud,
      fuente: fuente?.trim() || 'GPS',
      satelites: satelites ?? null,
      actualizadoEn: new Date(ahora).toISOString(),
    };
    io.to('frontend').emit('actualizacion_gps', ultimaUbicacionGps);
  });

  socket.on('video_stream', (payload) => {
    if (rol !== 'esp32' || typeof payload !== 'string') return;

    const ahora = Date.now();
    if (ahora - socket.data.ultimoFrameEn < MIN_FRAME_INTERVAL_MS) return;

    const frame = payload.startsWith('data:image/jpeg;base64,')
      ? payload.slice('data:image/jpeg;base64,'.length)
      : payload;

    if (
      frame.length === 0
      || frame.length > MAX_FRAME_BASE64_LENGTH
      || frame.length % 4 !== 0
      || !BASE64_JPEG_PATTERN.test(frame)
    ) {
      return;
    }

    socket.data.ultimoFrameEn = ahora;
    // volatile evita colas de frames viejos en navegadores con una conexión lenta.
    io.to('frontend').volatile.emit('actualizacion_video', frame);
  });

  socket.on('disconnect', (motivo) => {
    console.info(`[socket] desconectado rol=${rol} id=${socket.id} motivo=${motivo}`);

    // Prioriza seguridad: si un panel desaparece, el vehículo se detiene.
    if (rol === 'frontend') {
      io.to('raspberry').emit('comando_pi', 'STOP');
    }
  });
});

app.get('/health', (_request, response) => {
  response.status(200).json({
    ok: true,
    uptimeSeconds: Math.floor(process.uptime()),
    clients: {
      frontend: io.sockets.adapter.rooms.get('frontend')?.size || 0,
      raspberry: io.sockets.adapter.rooms.get('raspberry')?.size || 0,
      esp32: io.sockets.adapter.rooms.get('esp32')?.size || 0,
    },
  });
});

app.use(
  express.static(FRONTEND_DIR, {
    etag: true,
    maxAge: '1h',
    setHeaders(response, filePath) {
      if (path.basename(filePath) === 'index.html') {
        response.setHeader('Cache-Control', 'no-cache');
      }
    },
  }),
);

app.get('*', (request, response, next) => {
  if (!request.accepts('html')) {
    next();
    return;
  }
  response.sendFile(path.join(FRONTEND_DIR, 'index.html'));
});

app.use((_request, response) => {
  response.status(404).json({ error: 'Recurso no encontrado.' });
});

httpServer.listen(PORT, '0.0.0.0', () => {
  console.info(`[http] servidor escuchando en 0.0.0.0:${PORT}`);
  if (!DEVICE_TOKEN) {
    console.warn('[seguridad] DEVICE_TOKEN no está configurado; los dispositivos no requieren token.');
  }
});

let cerrando = false;
function apagar(signal) {
  if (cerrando) return;
  cerrando = true;
  console.info(`[sistema] señal ${signal}; deteniendo servicio.`);

  io.to('raspberry').emit('comando_pi', 'STOP');
  setTimeout(() => {
    io.disconnectSockets(true);
    httpServer.close(() => process.exit(0));
  }, 150).unref();

  setTimeout(() => process.exit(1), 5000).unref();
}

process.on('SIGTERM', () => apagar('SIGTERM'));
process.on('SIGINT', () => apagar('SIGINT'));
