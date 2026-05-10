/**
 * Worker push notifications — eseguito ogni 5 min da GitHub Actions cron.
 *
 * Flusso:
 *  1. Legge da Firestore /backend/state l'ultimo timestamp INGV controllato.
 *  2. Chiama API INGV FDSN per eventi sismici dopo quel timestamp.
 *  3. Per ogni nuovo evento, scorre /devices, calcola distanza Haversine,
 *     e se l'evento è dentro raggio + sopra magnitudo → invia FCM push.
 *  4. Aggiorna /backend/state con il timestamp dell'evento più recente.
 *
 * Env vars richieste:
 *  - GOOGLE_APPLICATION_CREDENTIALS_JSON: contenuto del service-account JSON (string)
 *  - oppure GOOGLE_APPLICATION_CREDENTIALS: path al file
 */
import admin from 'firebase-admin';
import { readFileSync } from 'node:fs';

// ── Init Firebase Admin ──────────────────────────────────────────────────────
function initFirebase() {
  if (admin.apps.length) return;
  let credential;
  if (process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON) {
    const json = JSON.parse(process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON);
    credential = admin.credential.cert(json);
  } else if (process.env.GOOGLE_APPLICATION_CREDENTIALS) {
    const json = JSON.parse(readFileSync(process.env.GOOGLE_APPLICATION_CREDENTIALS, 'utf8'));
    credential = admin.credential.cert(json);
  } else {
    throw new Error('Manca GOOGLE_APPLICATION_CREDENTIALS_JSON o GOOGLE_APPLICATION_CREDENTIALS');
  }
  admin.initializeApp({ credential });
}

// ── Distanza Haversine in km ────────────────────────────────────────────────
function haversineKm(lat1, lon1, lat2, lon2) {
  const R = 6371;
  const toRad = (d) => (d * Math.PI) / 180;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(a));
}

// ── Fetch INGV ──────────────────────────────────────────────────────────────
async function fetchIngvEvents(sinceIso) {
  // FDSN: starttime ISO, format=text è più leggero ma usiamo geojson per coerenza
  const url = `https://webservices.ingv.it/fdsnws/event/1/query?format=geojson&starttime=${encodeURIComponent(sinceIso)}&minmag=2.0&orderby=time-asc`;
  const r = await fetch(url, { headers: { 'User-Agent': 'CalendarioVVF-PushWorker/1.0' } });
  if (!r.ok) {
    if (r.status === 204 || r.status === 404) return [];
    throw new Error(`INGV ${r.status}`);
  }
  const j = await r.json();
  if (!j.features) return [];
  return j.features.map((f) => {
    const [lon, lat, depth] = f.geometry.coordinates;
    return {
      id: f.id ?? f.properties?.eventId ?? `${f.properties.time}-${lat}-${lon}`,
      time: f.properties.time, // ISO
      mag: f.properties.mag,
      magType: f.properties.magType,
      place: f.properties.place ?? '',
      lat,
      lon,
      depthKm: depth,
    };
  });
}

// ── Compose notifica ────────────────────────────────────────────────────────
function composeNotification(evt) {
  const magStr = evt.mag != null ? evt.mag.toFixed(1) : '?';
  const title = `⚠️ Sisma M${magStr}`;
  const placeShort = evt.place ? evt.place.replace(/^\d+\s*km\s+/i, '') : 'Italia';
  const depthStr = evt.depthKm != null ? ` · ${Math.round(evt.depthKm)}km` : '';
  const timeStr = (() => {
    try {
      return new Date(evt.time).toLocaleString('it-IT', {
        hour: '2-digit', minute: '2-digit', day: '2-digit', month: '2-digit',
      });
    } catch { return evt.time; }
  })();
  const body = `${placeShort}${depthStr} · ${timeStr}`;
  const color = evt.mag >= 5 ? '#C0392B' : evt.mag >= 4 ? '#E67E22' : '#F1C40F';
  return { title, body, color };
}

// ── Main ────────────────────────────────────────────────────────────────────
async function main() {
  initFirebase();
  const db = admin.firestore();
  const messaging = admin.messaging();

  // Stato (ultimo timestamp ISO controllato)
  const stateRef = db.collection('backend').doc('state');
  const stateSnap = await stateRef.get();
  const lastCheckedIso =
    stateSnap.exists && stateSnap.data().lastEventTime
      ? stateSnap.data().lastEventTime
      : new Date(Date.now() - 30 * 60 * 1000).toISOString(); // primo run: 30 min fa

  console.log(`[Worker] poll INGV from ${lastCheckedIso}`);
  const events = await fetchIngvEvents(lastCheckedIso);
  console.log(`[Worker] ${events.length} eventi`);

  if (events.length === 0) {
    await stateRef.set({ lastRun: new Date().toISOString() }, { merge: true });
    return;
  }

  // Filtra duplicati: skip eventi con time <= lastCheckedIso
  const lastTs = new Date(lastCheckedIso).getTime();
  const newEvents = events.filter((e) => new Date(e.time).getTime() > lastTs);
  console.log(`[Worker] ${newEvents.length} nuovi`);

  if (newEvents.length === 0) {
    await stateRef.set({ lastRun: new Date().toISOString() }, { merge: true });
    return;
  }

  // Carica device registrati
  const devicesSnap = await db.collection('devices').get();
  const devices = devicesSnap.docs.map((d) => ({ id: d.id, ...d.data() }));
  console.log(`[Worker] ${devices.length} device registrati`);

  let pushSent = 0;
  let pushFailed = 0;
  const tokensToRemove = [];

  for (const evt of newEvents) {
    for (const dev of devices) {
      if (!dev.earthquake?.enabled) continue;
      if (evt.mag < dev.earthquake.minMag) continue;
      const dist = haversineKm(dev.lat, dev.lon, evt.lat, evt.lon);
      if (dist > dev.earthquake.radiusKm) continue;

      // Match → invia push
      const { title, body, color } = composeNotification(evt);
      const msg = {
        token: dev.token,
        notification: { title, body },
        data: {
          eventId: String(evt.id),
          mag: String(evt.mag ?? ''),
          lat: String(evt.lat),
          lon: String(evt.lon),
          color,
          type: 'earthquake',
        },
        android: {
          priority: 'high',
          notification: {
            channelId: 'vvf_push_alert_v1',
            sound: 'default',
            color,
            defaultVibrateTimings: true,
            visibility: 'public',
          },
        },
      };
      try {
        await messaging.send(msg);
        pushSent++;
      } catch (e) {
        pushFailed++;
        const code = e?.errorInfo?.code ?? e?.code ?? '';
        if (code.includes('registration-token-not-registered') || code.includes('invalid-argument')) {
          tokensToRemove.push(dev.token);
        }
        console.log(`[Worker] push fail token=${dev.token.slice(0,16)} code=${code}`);
      }
    }
  }

  // Pulizia token invalidi
  if (tokensToRemove.length) {
    const batch = db.batch();
    for (const t of tokensToRemove) batch.delete(db.collection('devices').doc(t));
    await batch.commit();
    console.log(`[Worker] rimossi ${tokensToRemove.length} token invalidi`);
  }

  // Aggiorna stato
  const newest = newEvents[newEvents.length - 1].time;
  await stateRef.set({ lastEventTime: newest, lastRun: new Date().toISOString() }, { merge: true });
  console.log(`[Worker] done. push ok=${pushSent} fail=${pushFailed} state=${newest}`);
}

main().catch((e) => {
  console.error('[Worker] fatal:', e);
  process.exit(1);
});
