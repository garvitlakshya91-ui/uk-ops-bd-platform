'use client';

import { MapContainer, TileLayer, CircleMarker } from 'react-leaflet';
import Link from 'next/link';
import 'leaflet/dist/leaflet.css';

/** Small static-feel location map for an asset detail panel.
 *  Client-only — import via next/dynamic with ssr: false. */
export default function MiniMap({
  lat,
  lng,
  color = '#F2A65A',
}: {
  lat: number;
  lng: number;
  color?: string;
}) {
  return (
    <div className="relative rounded-xl overflow-hidden border border-white/[0.08] h-44">
      <MapContainer
        center={[lat, lng]}
        zoom={15}
        className="h-full w-full"
        zoomControl={false}
        scrollWheelZoom={false}
        dragging={false}
        doubleClickZoom={false}
        attributionControl={false}
        preferCanvas
      >
        <TileLayer url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png" />
        <CircleMarker
          center={[lat, lng]}
          radius={9}
          pathOptions={{
            color: 'rgba(255,244,230,0.9)',
            weight: 2,
            fillColor: color,
            fillOpacity: 0.95,
          }}
        />
      </MapContainer>
      <Link
        href="/map"
        className="absolute bottom-2 right-2 z-[500] px-2.5 py-1 rounded-lg text-[11px] font-semibold bg-black/60 text-slate-200 backdrop-blur-md border border-white/[0.1] hover:bg-black/80 hover:text-white transition-colors cursor-pointer"
      >
        Open full map →
      </Link>
    </div>
  );
}
