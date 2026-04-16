"use client";

import { useEffect, useRef, useState } from "react";
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

function paraFeatureCollection(data: object): GeoJSON.FeatureCollection {
  const anyData = data as { type?: string };
  if (anyData?.type === "FeatureCollection") {
    return data as GeoJSON.FeatureCollection;
  }
  if (anyData?.type === "Feature") {
    return {
      type: "FeatureCollection",
      features: [data as GeoJSON.Feature],
    };
  }
  return {
    type: "FeatureCollection",
    features: [
      {
        type: "Feature",
        properties: {},
        geometry: data as GeoJSON.Geometry,
      },
    ],
  };
}

function upsertGeoJsonSource(
  map: maplibregl.Map,
  sourceId: string,
  data: GeoJSON.FeatureCollection
) {
  const src = map.getSource(sourceId) as maplibregl.GeoJSONSource | undefined;
  if (src) {
    src.setData(data);
    return;
  }
  map.addSource(sourceId, {
    type: "geojson",
    data,
  });
}

export default function MapView({ resultado }: MapViewProps) {
  const mapContainer = useRef<HTMLDivElement>(null);
  const map          = useRef<maplibregl.Map | null>(null);
  const [mapError, setMapError] = useState<string | null>(null);
  const pendingFlyTarget = useRef<{ lat: number; lng: number } | null>(null);
  const lastFlyKey = useRef<string | null>(null);

  const flyToPendingTarget = () => {
    const m = map.current;
    const target = pendingFlyTarget.current;
    if (!m || !target || !m.isStyleLoaded()) return;

    const flyKey = `${target.lat.toFixed(6)},${target.lng.toFixed(6)}`;
    if (flyKey === lastFlyKey.current) return;

    lastFlyKey.current = flyKey;
    m.stop();
    m.resize();
    m.flyTo({ center: [target.lng, target.lat], zoom: 15, duration: 1200 });
    window.setTimeout(() => {
      m.resize();
    }, 320);
  };

  // Inicializa o mapa uma vez
  useEffect(() => {
    if (!mapContainer.current || map.current) return;

    try {
      map.current = new maplibregl.Map({
        container: mapContainer.current,
        style: "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
        center: [-43.1729, -22.9068],
        zoom: 11,
        attributionControl: false,
      });
    } catch {
      window.setTimeout(() => {
        setMapError("Não foi possível inicializar o mapa.");
      }, 0);
      return;
    }

    const m = map.current;
    const timeoutId = window.setTimeout(() => {
      if (m && !m.isStyleLoaded()) {
        setMapError("Mapa base indisponível no momento.");
      }
    }, 7000);

    const onLoad = () => {
      window.clearTimeout(timeoutId);
      setMapError(null);
      flyToPendingTarget();
    };

    const onError = () => {
      if (m && !m.isStyleLoaded()) {
        setMapError("Mapa base indisponível no momento.");
      }
    };

    m.on("load", onLoad);
    m.on("error", onError);

    m.addControl(
      new maplibregl.NavigationControl({ showCompass: false }),
      "top-right"
    );

    return () => {
      window.clearTimeout(timeoutId);
      m.off("load", onLoad);
      m.off("error", onError);
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

    if (mapError) return;

    const removeLayers = () => {
      if (m.getLayer(LAYER_SETORES_FILL)) m.removeLayer(LAYER_SETORES_FILL);
      if (m.getLayer(LAYER_SETORES_LINE)) m.removeLayer(LAYER_SETORES_LINE);
      if (m.getLayer(LAYER_RUA_LINE)) m.removeLayer(LAYER_RUA_LINE);
      if (m.getLayer(LAYER_RUA_CIRCLE)) m.removeLayer(LAYER_RUA_CIRCLE);
      if (m.getSource(SOURCE_SETORES)) m.removeSource(SOURCE_SETORES);
      if (m.getSource(SOURCE_RUA)) m.removeSource(SOURCE_RUA);
    };

    // Sem resultado — limpa o mapa
    if (!resultado || lat === undefined || lng === undefined) {
      if (m.isStyleLoaded()) removeLayers();
      else m.once("load", removeLayers);
      return;
    }

    const latNum = Number(lat);
    const lngNum = Number(lng);
    if (!Number.isFinite(latNum) || !Number.isFinite(lngNum)) {
      return;
    }

    pendingFlyTarget.current = { lat: latNum, lng: lngNum };
    flyToPendingTarget();

    const addLayers = () => {
      try {
        // 1. Setores censitários (verde)
        if (resultado.geojson) {
          upsertGeoJsonSource(m, SOURCE_SETORES, paraFeatureCollection(resultado.geojson));

          if (!m.getLayer(LAYER_SETORES_FILL)) {
            m.addLayer({
              id: LAYER_SETORES_FILL,
              type: "fill",
              source: SOURCE_SETORES,
              paint: { "fill-color": "#00e5b0", "fill-opacity": 0.15 },
            });
          }

          if (!m.getLayer(LAYER_SETORES_LINE)) {
            m.addLayer({
              id: LAYER_SETORES_LINE,
              type: "line",
              source: SOURCE_SETORES,
              paint: { "line-color": "#00c49a", "line-width": 1.5, "line-opacity": 0.8 },
            });
          }
        }

        // 2. Geometria da rua (escuro)
        if (resultado.geometria) {
          upsertGeoJsonSource(m, SOURCE_RUA, paraFeatureCollection(resultado.geometria));

          if (!m.getLayer(LAYER_RUA_LINE)) {
            m.addLayer({
              id: LAYER_RUA_LINE,
              type: "line",
              source: SOURCE_RUA,
              layout: { "line-cap": "round", "line-join": "round" },
              paint: { "line-color": "#1E293B", "line-width": 4 },
            });
          }

          if (!m.getLayer(LAYER_RUA_CIRCLE)) {
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
        }
      } catch (err) {
        // Falha de renderização não deve impedir o flyTo da câmera.
        console.error("Falha ao atualizar layers do mapa", err);
      }
    };

    if (m.isStyleLoaded()) addLayers();
    else m.once("load", addLayers);

  }, [resultado, mapError]);

  return (
    <div style={{ width: "100%", height: "100%", borderRadius: "inherit", position: "relative" }}>
      <div
        ref={mapContainer}
        style={{ width: "100%", height: "100%", borderRadius: "inherit" }}
      />

      {mapError && (
        <div style={{
          position: "absolute",
          inset: 0,
          background: "rgba(255,255,255,0.92)",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          textAlign: "center",
          padding: 24,
        }}>
          <div>
            <p style={{ fontSize: 14, fontWeight: 600, color: "#1a1a1a", marginBottom: 6 }}>
              Visualização de mapa indisponível
            </p>
            <p style={{ fontSize: 13, color: "rgba(26,26,26,0.6)" }}>
              {mapError} Os dados de renda continuam disponíveis no painel.
            </p>
          </div>
        </div>
      )}
    </div>
  );
}