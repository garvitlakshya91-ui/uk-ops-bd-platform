'use client';

import dynamic from 'next/dynamic';

// Leaflet touches `window` at import time — client-only.
const MapView = dynamic(() => import('./components/MapView'), {
  ssr: false,
  loading: () => (
    <div className="h-[calc(100vh-7.5rem)] rounded-2xl glass-card flex items-center justify-center">
      <p className="text-sm text-slate-400">Loading map…</p>
    </div>
  ),
});

export default function MapPage() {
  return (
    <div>
      <div className="mb-4">
        <h1 className="text-2xl font-bold text-white">Property Map</h1>
        <p className="text-sm text-slate-400 mt-1">
          Every scheme with coordinates — cluster, filter, and colour by type
          or operator health.
        </p>
      </div>
      <MapView />
    </div>
  );
}
