"use client";

import { useEffect, useRef } from "react";
import maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";

export interface MapPoint {
  latitude: number;
  longitude: number;
  estado?: string;
  geojson?: object;
  geometria?: object;
}

interface MapViewProps {
  resultado: MapPoint | null;
}

const LAYER_SETORES_FILL = "setores-fill";
const LAYER_SETORES_LINE = "setores-line";
const SOURCE_SETORES     = "setores-source";

const LAYER_RUA_LINE   = "rua-line";
const LAYER_RUA_CIRCLE = "rua-circle";
const SOURCE_RUA       = "rua-source";

export default function MapView({ resultado }: MapViewProps) {
  const mapContainer = useRef<HTMLDivElement>(null);
  const map          = useRef<maplibregl.Map | null>(null);

  // Inicializa o mapa uma vez
  useEffect(() => {
    if (!mapContainer.current || map.current) return;

    map.current = new maplibregl.Map({
      container: mapContainer.current,
      style: "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
      center: [-43.1729, -22.9068],
      zoom: 11,
      attributionControl: false,
    });

    map.current.addControl(
      new maplibregl.NavigationControl({ showCompass: false }),
      "top-right"
    );

    return () => {
      map.current?.remove();
      map.current = null;
    };
  }, []);

  // Reage a mudanças no resultado
  useEffect(() => {
    const m = map.current;
    if (!m) return;

    // Captura coordenadas agora — antes de qualquer async
    const lat = resultado?.latitude;
    const lng = resultado?.longitude;

    const removeLayers = () => {
      if (m.getLayer(LAYER_SETORES_FILL)) m.removeLayer(LAYER_SETORES_FILL);
      if (m.getLayer(LAYER_SETORES_LINE)) m.removeLayer(LAYER_SETORES_LINE);
      if (m.getLayer(LAYER_RUA_LINE))     m.removeLayer(LAYER_RUA_LINE);
      if (m.getLayer(LAYER_RUA_CIRCLE))   m.removeLayer(LAYER_RUA_CIRCLE);
      if (m.getSource(SOURCE_SETORES))    m.removeSource(SOURCE_SETORES);
      if (m.getSource(SOURCE_RUA))        m.removeSource(SOURCE_RUA);
    };

    // Sem resultado — limpa o mapa
    if (!resultado || lat === undefined || lng === undefined) {
      if (m.loaded()) removeLayers();
      else m.once("load", removeLayers);
      return;
    }

    const addLayers = () => {
      removeLayers();

      // 1. Setores censitários (verde)
      if (resultado.geojson) {
        m.addSource(SOURCE_SETORES, {
          type: "geojson",
          data: resultado.geojson as GeoJSON.FeatureCollection,
        });

        m.addLayer({
          id: LAYER_SETORES_FILL,
          type: "fill",
          source: SOURCE_SETORES,
          paint: { "fill-color": "#00e5b0", "fill-opacity": 0.15 },
        });

        m.addLayer({
          id: LAYER_SETORES_LINE,
          type: "line",
          source: SOURCE_SETORES,
          paint: { "line-color": "#00c49a", "line-width": 1.5, "line-opacity": 0.8 },
        });
      }

      // 2. Geometria da rua (escuro)
      if (resultado.geometria) {
        m.addSource(SOURCE_RUA, {
          type: "geojson",
          data: resultado.geometria as GeoJSON.Geometry,
        });

        // Layer de linha — sem filtro, type "line" já só renderiza linhas
        m.addLayer({
          id: LAYER_RUA_LINE,
          type: "line",
          source: SOURCE_RUA,
          layout: { "line-cap": "round", "line-join": "round" },
          paint: { "line-color": "#1E293B", "line-width": 4 },
        });

        // Layer de ponto — filtro com sintaxe moderna do MapLibre
        m.addLayer({
          id: LAYER_RUA_CIRCLE,
          type: "circle",
          source: SOURCE_RUA,
          filter: ["==", ["geometry-type"], "Point"],
          paint: {
            "circle-radius": 7,
            "circle-color": "#1E293B",
            "circle-stroke-width": 2,
            "circle-stroke-color": "#ffffff",
          },
        });
      }

      // flyTo com coordenadas capturadas no início do effect
      // Evita closure stale — usa lat/lng fixados antes de qualquer await
      m.flyTo({ center: [lng, lat], zoom: 15, duration: 1200 });
    };

    addLayers();

  }, [resultado]);

  return (
    <div
      ref={mapContainer}
      style={{ width: "100%", height: "100%", borderRadius: "inherit" }}
    />
  );
}