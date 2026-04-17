"use client";

import { useCallback, useEffect, useRef, useState } from "react";
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
const SOURCE_SETORES = "setores-source";

const LAYER_RUA_LINE = "rua-line";
const LAYER_RUA_CIRCLE = "rua-circle";
const SOURCE_RUA = "rua-source";
const MAP_DEBUG = true;

export default function MapView({ resultado }: MapViewProps) {
  const mapContainer = useRef<HTMLDivElement>(null);
  const map = useRef<maplibregl.Map | null>(null);
  const [mapError, setMapError] = useState<string | null>(null);
  const [isMapReady, setIsMapReady] = useState(false); // Novo estado para controle reativo

  const logMap = useCallback((...args: unknown[]) => {
    if (!MAP_DEBUG) return;
    console.debug("[MapView]", ...args);
  }, []);

  // Inicialização do Mapa
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

      const m = map.current;

      // Garante que o mapa se ajuste ao tamanho da div automaticamente
      const resizer = new ResizeObserver(() => m.resize());
      resizer.observe(mapContainer.current);

      m.on("load", () => {
        logMap("map load event");
        setIsMapReady(true);
      });

      m.on("error", (e) => {
        logMap("map error event", e);
        if (!m.isStyleLoaded()) setMapError("Mapa base indisponivel.");
      });

      m.addControl(new maplibregl.NavigationControl({ showCompass: false }), "top-right");

      return () => {
        resizer.disconnect();
        m.remove();
        map.current = null;
      };
    } catch (err) {
      setMapError("Erro ao inicializar o mapa.");
    }
  }, [logMap]);

  // Efeito principal: Atualiza camadas e voa para o alvo quando mapReady E resultado existem
  useEffect(() => {
    const m = map.current;
    if (!m || !isMapReady || !resultado) return;

    const lat = Number(resultado.latitude);
    const lng = Number(resultado.longitude);
    if (!Number.isFinite(lat) || !Number.isFinite(lng)) return;

    const applyUpdate = () => {
      try {
        // 1. Limpeza de camadas anteriores
        if (m.getLayer(LAYER_SETORES_FILL)) m.removeLayer(LAYER_SETORES_FILL);
        if (m.getLayer(LAYER_SETORES_LINE)) m.removeLayer(LAYER_SETORES_LINE);
        if (m.getLayer(LAYER_RUA_LINE)) m.removeLayer(LAYER_RUA_LINE);
        if (m.getLayer(LAYER_RUA_CIRCLE)) m.removeLayer(LAYER_RUA_CIRCLE);
        if (m.getSource(SOURCE_SETORES)) m.removeSource(SOURCE_SETORES);
        if (m.getSource(SOURCE_RUA)) m.removeSource(SOURCE_RUA);

        // 2. Voo para a localização (FlyTo)
        m.flyTo({
          center: [lng, lat],
          zoom: 15,
          duration: 1200,
          essential: true
        });

        // 3. Adicionar Setores (GeoJSON)
        if (resultado.geojson) {
          m.addSource(SOURCE_SETORES, {
            type: "geojson",
            data: resultado.geojson as any,
          });
          m.addLayer({
            id: LAYER_SETORES_FILL,
            type: "fill",
            source: SOURCE_SETORES,
            paint: { "fill-color": "#00e5b0", "fill-opacity": 0.25 },
          });
          m.addLayer({
            id: LAYER_SETORES_LINE,
            type: "line",
            source: SOURCE_SETORES,
            paint: { "line-color": "#00c49a", "line-width": 2 },
          });
        }

        // 4. Adicionar Linha da Rua (Geometria)
        if (resultado.geometria) {
          const feature: any = {
            type: "Feature",
            geometry: resultado.geometria,
            properties: {},
          };
          m.addSource(SOURCE_RUA, {
            type: "geojson",
            data: { type: "FeatureCollection", features: [feature] },
          });
          m.addLayer({
            id: LAYER_RUA_LINE,
            type: "line",
            source: SOURCE_RUA,
            layout: { "line-cap": "round", "line-join": "round" },
            paint: { "line-color": "#1E293B", "line-width": 4 },
          });
        }

        logMap("Update aplicado com sucesso");
      } catch (error) {
        console.error("[MapView] Erro ao atualizar mapa:", error);
      }
    };

    // Pequeno atraso (requestAnimationFrame) para garantir que o container esteja renderizado
    requestAnimationFrame(applyUpdate);

  }, [isMapReady, resultado, logMap]);

  return (
    <div style={{ width: "100%", height: "100%", borderRadius: "inherit", position: "relative" }}>
      <div ref={mapContainer} style={{ width: "100%", height: "100%", borderRadius: "inherit" }} />
      {mapError && (
        <div style={{
          position: "absolute", inset: 0, background: "rgba(255,255,255,0.9)",
          display: "flex", alignItems: "center", justifyContent: "center", textAlign: "center", padding: 20
        }}>
          <div>
            <p style={{ fontWeight: 600 }}>Visualização indisponível</p>
            <p style={{ fontSize: 13, opacity: 0.7 }}>{mapError}</p>
          </div>
        </div>
      )}
    </div>
  );
}