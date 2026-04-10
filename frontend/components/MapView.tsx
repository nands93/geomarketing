"use client";

import { useEffect, useRef, useState } from "react";
import maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";

interface SetorGeo {
  latitude: number;
  longitude: number;
  setor_censitario?: string;
}

interface MapViewProps {
  resultado: SetorGeo | null;
}

export default function MapView({ resultado }: MapViewProps) {
  const mapContainer = useRef<HTMLDivElement>(null);
  const map = useRef<maplibregl.Map | null>(null);
  const marker = useRef<maplibregl.Marker | null>(null);
  type StateCode = 'RJ' | 'SP';

  const STATE_CENTERS: Record<StateCode, [number, number]> = {
  'RJ': [-43.1729, -22.9068],  // Rio de Janeiro
  'SP': [-46.6333, -23.5505]   // São Paulo
};

  const [selectedState, setSelectedState] = useState<StateCode>('RJ');

  // Inicializa o mapa uma vez
  useEffect(() => {
    if (!mapContainer.current || map.current) return;

    map.current = new maplibregl.Map({
      container: mapContainer.current,
      style: "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
      center: STATE_CENTERS[selectedState],
      zoom: 11,
    });

    map.current.addControl(new maplibregl.NavigationControl(), "top-right");

    return () => {
      map.current?.remove();
      map.current = null;
    };
  }, []);

  // Atualiza o marcador quando chega um resultado
  useEffect(() => {
    if (!map.current || !resultado) return;

    // Remove marcador anterior
    marker.current?.remove();

    // Cria novo marcador
    const el = document.createElement("div");
    el.style.cssText = `
      width: 16px;
      height: 16px;
      background: #00e5b0;
      border: 2px solid #fff;
      border-radius: 50%;
      box-shadow: 0 0 12px #00e5b0;
    `;

    marker.current = new maplibregl.Marker({ element: el })
      .setLngLat([resultado.longitude, resultado.latitude])
      .addTo(map.current);

    // Voa para o ponto
    map.current.flyTo({
      center: [resultado.longitude, resultado.latitude],
      zoom: 14,
      duration: 1200,
    });
  }, [resultado]);

  return (
    <div
      ref={mapContainer}
      className="w-full h-full rounded-lg overflow-hidden"
      style={{ minHeight: "400px" }}
    />
  );
}
