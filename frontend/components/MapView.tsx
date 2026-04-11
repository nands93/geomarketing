"use client";

import { useEffect, useRef } from "react";
import maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";

export interface MapPoint {
  latitude: number;
  longitude: number;
  estado?: string;
  geojson?: object;
}

interface MapViewProps {
  resultado: MapPoint | null;
}

const LAYER_FILL = "setor-fill";
const LAYER_LINE = "setor-line";
const SOURCE_ID  = "setor";

export default function MapView({ resultado }: MapViewProps) {
  const mapContainer = useRef<HTMLDivElement>(null);
  const map          = useRef<maplibregl.Map | null>(null);

  useEffect(() => {
    if (!mapContainer.current || map.current) return;

    map.current = new maplibregl.Map({
      container: mapContainer.current,
      // Estilo claro para combinar com o design system
      style: "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
      center: [-43.1729, -22.9068],
      zoom: 11,
    });

    map.current.addControl(new maplibregl.NavigationControl(), "top-right");

    return () => {
      map.current?.remove();
      map.current = null;
    };
  }, []);

  useEffect(() => {
    const m = map.current;
    if (!m || !resultado) return;

    m.flyTo({ center: [resultado.longitude, resultado.latitude], zoom: 14, duration: 1000 });

    if (!resultado.geojson) return;

    const addLayers = () => {
      if (m.getLayer(LAYER_FILL)) m.removeLayer(LAYER_FILL);
      if (m.getLayer(LAYER_LINE)) m.removeLayer(LAYER_LINE);
      if (m.getSource(SOURCE_ID)) m.removeSource(SOURCE_ID);

      m.addSource(SOURCE_ID, {
        type: "geojson",
        data: resultado.geojson as GeoJSON.FeatureCollection,
      });

      m.addLayer({
        id: LAYER_FILL,
        type: "fill",
        source: SOURCE_ID,
        paint: { "fill-color": "#00e5b0", "fill-opacity": 0.18 },
      });

      m.addLayer({
        id: LAYER_LINE,
        type: "line",
        source: SOURCE_ID,
        paint: { "line-color": "#00c49a", "line-width": 2, "line-opacity": 1 },
      });
    };

    if (m.isStyleLoaded()) addLayers();
    else m.once("styledata", addLayers);
  }, [resultado]);

  return (
    <div ref={mapContainer} style={{ width: "100%", height: "100%" }} />
  );
}