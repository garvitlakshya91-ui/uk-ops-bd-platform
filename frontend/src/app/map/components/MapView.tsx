'use client';

import { useEffect, useMemo, useState } from 'react';
import { MapContainer, TileLayer, CircleMarker, Popup, ZoomControl } from 'react-leaflet';
import MarkerClusterGroup from 'react-leaflet-cluster';
import Link from 'next/link';
import 'leaflet/dist/leaflet.css';
import api from '@/lib/api';

interface MapScheme {
  i: number;      // id
  n: string;      // name
  la: number;     // lat
  ln: number;     // lng
  t: string;      // scheme_type
  u: number | null;   // units
  b: number | null;   // bd_score
  h: number | null;   // health / arrears score
  c: string | null;   // council
}

const TYPE_COLORS: Record<string, string> = {
  BTR: '#5EB1FF',
  PBSA: '#F2A65A',
  'Co-living': '#B197D6',
  Senior: '#30A46C',
};
const DEFAULT_COLOR = '#9A8E7D';

function healthColor(h: number | null): string {
  if (h === null || h === undefined) return DEFAULT_COLOR;
  if (h >= 80) return '#E5484D';
  if (h >= 60) return '#F76B15';
  if (h >= 35) return '#F2A65A';
  return '#30A46C';
}

export default function MapView() {
  const [scope, setScope] = useState<'bd' | 'all'>('bd');
  const [colorBy, setColorBy] = useState<'type' | 'health'>('type');
  const [typeFilter, setTypeFilter] = useState<Set<string>>(
    new Set(['BTR', 'PBSA', 'Co-living', 'Senior'])
  );
  const [schemes, setSchemes] = useState<MapScheme[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    api
      .get(`/v2/map/schemes?scope=${scope}`)
      .then((r) => {
        if (!cancelled) setSchemes(r.data.schemes);
      })
      .finally(() => !cancelled && setLoading(false));
    return () => {
      cancelled = true;
    };
  }, [scope]);

  const visible = useMemo(
    () =>
      scope === 'all'
        ? schemes
        : schemes.filter((s) => typeFilter.has(s.t)),
    [schemes, typeFilter, scope]
  );

  return (
    <div className="relative h-[calc(100vh-7.5rem)] rounded-2xl overflow-hidden glass-card">
      {/* Controls */}
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
                {s === 'bd' ? 'BD schemes' : 'All properties'}
              </button>
            ))}
          </div>
          <div className="flex items-center gap-2">
            {(['type', 'health'] as const).map((m) => (
              <button
                key={m}
                onClick={() => setColorBy(m)}
                className={`px-3 py-1 rounded-lg text-xs font-medium transition-colors cursor-pointer ${
                  colorBy === m
                    ? 'bg-white/[0.14] text-white'
                    : 'bg-white/[0.04] text-slate-400 hover:bg-white/[0.1]'
                }`}
              >
                Colour: {m === 'type' ? 'Type' : 'Health'}
              </button>
            ))}
          </div>
          {scope === 'bd' && (
            <div className="flex flex-wrap gap-1.5 pt-1 border-t border-white/[0.07]">
              {Object.entries(TYPE_COLORS).map(([t, col]) => (
                <button
                  key={t}
                  onClick={() =>
                    setTypeFilter((prev) => {
                      const next = new Set(prev);
                      if (next.has(t)) next.delete(t);
                      else next.add(t);
                      return next;
                    })
                  }
                  className={`flex items-center gap-1.5 px-2 py-0.5 rounded-md text-[11px] transition-opacity cursor-pointer ${
                    typeFilter.has(t) ? 'opacity-100' : 'opacity-35'
                  } bg-white/[0.05] text-slate-200`}
                >
                  <span
                    className="w-2 h-2 rounded-full"
                    style={{ background: col }}
                  />
                  {t}
                </button>
              ))}
            </div>
          )}
        </div>
        <div className="glass-card px-3 py-1.5 text-[11px] text-slate-400">
          {loading ? 'Loading…' : `${visible.length.toLocaleString()} properties`}
        </div>
      </div>

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
          {visible.map((s) => (
            <CircleMarker
              key={s.i}
              center={[s.la, s.ln]}
              radius={6}
              pathOptions={{
                color: 'rgba(255,244,230,0.35)',
                weight: 1,
                fillColor:
                  colorBy === 'health'
                    ? healthColor(s.h)
                    : TYPE_COLORS[s.t] || DEFAULT_COLOR,
                fillOpacity: 0.85,
              }}
            >
              <Popup>
                <div style={{ minWidth: 190 }}>
                  <div style={{ fontWeight: 700, marginBottom: 2 }}>{s.n}</div>
                  <div style={{ fontSize: 12, opacity: 0.85 }}>
                    {s.t}
                    {s.c ? ` · ${s.c}` : ''}
                    {s.u ? ` · ${s.u} units` : ''}
                  </div>
                  <div style={{ fontSize: 12, marginTop: 4 }}>
                    {s.b != null && <span>BD score {Math.round(s.b)} · </span>}
                    {s.h != null && <span>Health {Math.round(s.h)}</span>}
                  </div>
                  <Link
                    href={`/schemes?id=${s.i}`}
                    style={{ fontSize: 12, fontWeight: 600 }}
                  >
                    Open scheme →
                  </Link>
                </div>
              </Popup>
            </CircleMarker>
          ))}
        </MarkerClusterGroup>
      </MapContainer>
    </div>
  );
}
