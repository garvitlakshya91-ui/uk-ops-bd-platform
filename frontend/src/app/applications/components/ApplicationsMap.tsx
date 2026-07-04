'use client';

import { useEffect, useMemo, useState } from 'react';
import { MapContainer, TileLayer, CircleMarker, Popup, ZoomControl } from 'react-leaflet';
import MarkerClusterGroup from 'react-leaflet-cluster';
import 'leaflet/dist/leaflet.css';
import api from '@/lib/api';

interface MapApp {
  i: string;   // id
  n: string;   // address
  la: number;  // lat
  ln: number;  // lng
  t: string | null;   // scheme_type
  u: number | null;   // units
  b: number | null;   // bd_score
  s: string | null;   // status
  r: string | null;   // reference
  c: string | null;   // council
}

const TYPE_COLORS: Record<string, string> = {
  BTR: '#5EB1FF',
  PBSA: '#F2A65A',
  'Co-living': '#B197D6',
  Senior: '#30A46C',
  Residential: '#9A8E7D',
};
const DEFAULT_COLOR = '#9A8E7D';

function bdColor(b: number | null): string {
  if (b === null || b === undefined) return DEFAULT_COLOR;
  if (b >= 70) return '#E5484D';
  if (b >= 50) return '#F2A65A';
  if (b >= 30) return '#F6BA71';
  return '#5EB1FF';
}

export default function ApplicationsMap() {
  const [scope, setScope] = useState<'bd' | 'all'>('bd');
  const [colorBy, setColorBy] = useState<'type' | 'bd'>('bd');
  const [apps, setApps] = useState<MapApp[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    api
      .get(`/v2/map/applications?scope=${scope}`)
      .then((r) => {
        if (!cancelled) setApps(r.data.applications);
      })
      .finally(() => !cancelled && setLoading(false));
    return () => {
      cancelled = true;
    };
  }, [scope]);

  const controls = useMemo(
    () => (
      <div className="absolute top-3 left-3 z-[1000] flex flex-col gap-2">
        <div className="glass-card-premium px-4 py-3 flex flex-col gap-2">
          <div className="flex items-center gap-2">
            {(['bd', 'all'] as const).map((s) => (
              <button
                key={s}
                onClick={() => setScope(s)}
                className={`px-3 py-1 rounded-lg text-xs font-semibold transition-colors cursor-pointer ${
                  scope === s
                    ? 'bg-ember-500/90 text-white'
                    : 'bg-white/[0.06] text-slate-300 hover:bg-white/[0.12]'
                }`}
              >
                {s === 'bd' ? 'BD-scored' : 'All geocoded'}
              </button>
            ))}
          </div>
          <div className="flex items-center gap-2">
            {(['bd', 'type'] as const).map((m) => (
              <button
                key={m}
                onClick={() => setColorBy(m)}
                className={`px-3 py-1 rounded-lg text-xs font-medium transition-colors cursor-pointer ${
                  colorBy === m
                    ? 'bg-white/[0.14] text-white'
                    : 'bg-white/[0.04] text-slate-400 hover:bg-white/[0.1]'
                }`}
              >
                Colour: {m === 'bd' ? 'BD score' : 'Type'}
              </button>
            ))}
          </div>
        </div>
        <div className="glass-card px-3 py-1.5 text-[11px] text-slate-400">
          {loading ? 'Loading…' : `${apps.length.toLocaleString()} applications`}
        </div>
      </div>
    ),
    [scope, colorBy, loading, apps.length]
  );

  return (
    <div className="relative h-[70vh] rounded-2xl overflow-hidden glass-card">
      {controls}
      <MapContainer
        center={[54.2, -2.5]}
        zoom={6}
        className="h-full w-full"
        preferCanvas
        zoomControl={false}
      >
        <ZoomControl position="topright" />
        <TileLayer
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> &copy; <a href="https://carto.com/">CARTO</a>'
          url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
        />
        <MarkerClusterGroup chunkedLoading maxClusterRadius={55}>
          {apps.map((a) => (
            <CircleMarker
              key={a.i}
              center={[a.la, a.ln]}
              radius={6}
              pathOptions={{
                color: 'rgba(255,244,230,0.35)',
                weight: 1,
                fillColor:
                  colorBy === 'bd'
                    ? bdColor(a.b)
                    : TYPE_COLORS[a.t || ''] || DEFAULT_COLOR,
                fillOpacity: 0.85,
              }}
            >
              <Popup>
                <div style={{ minWidth: 200 }}>
                  <div style={{ fontFamily: 'monospace', fontSize: 11, opacity: 0.8 }}>
                    {a.r}
                  </div>
                  <div style={{ fontWeight: 700, marginTop: 2 }}>{a.n}</div>
                  <div style={{ fontSize: 12, opacity: 0.85, marginTop: 2 }}>
                    {a.t}
                    {a.c ? ` · ${a.c}` : ''}
                    {a.u ? ` · ${a.u} units` : ''}
                  </div>
                  <div style={{ fontSize: 12, marginTop: 4 }}>
                    {a.b != null && <span>BD score {Math.round(a.b)}</span>}
                    {a.s ? ` · ${a.s}` : ''}
                  </div>
                </div>
              </Popup>
            </CircleMarker>
          ))}
        </MarkerClusterGroup>
      </MapContainer>
    </div>
  );
}
