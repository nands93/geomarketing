"use client";

import { useState } from "react";
import dynamic from "next/dynamic";
import type { MapPoint } from "@/components/MapView";

const MapView = dynamic<{ resultado: MapPoint | null }>(
  () => import("@/components/MapView"),
  { ssr: false }
);

const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

// ─── Tipos ────────────────────────────────────────────────────────────────────

interface Endereco {
  logradouro: string;
  bairro: string;
  cidade: string;
  estado: string;
  latitude: number;
  longitude: number;
  geometria?: object;
  geometria_tipo?: string;
}

interface Resumo {
  total_setores: number;
  renda_mediana: number;
  renda_media_minima: number;
  renda_media_maxima: number;
  classe_predominante: string;
}

interface Setor {
  setor_censitario: string;
  renda_media: number;
  classe_social_estimada: string;
}

interface ResultadoCEP {
  cep: string;
  endereco: Endereco;
  resumo: Resumo;
  setores: Setor[];
  geojson: object;
  fonte_geocoding: string;
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

function formatarRenda(valor: number) {
  return valor.toLocaleString("pt-BR", {
    style: "currency",
    currency: "BRL",
    maximumFractionDigits: 0,
  });
}

function formatarCEP(valor: string) {
  const n = valor.replace(/\D/g, "").slice(0, 8);
  return n.length > 5 ? `${n.slice(0, 5)}-${n.slice(5)}` : n;
}

const CLASSE_COR: Record<string, string> = {
  A:     "#00e5b0",
  B:     "#00e5b0",
  C:     "#f5a623",
  "D/E": "#e05c5c",
};

// ─── Componente ───────────────────────────────────────────────────────────────

export default function Home() {
  const [cep, setCep]          = useState("");
  const [loading, setLoading]  = useState(false);
  const [resultado, setResult] = useState<ResultadoCEP | null>(null);
  const [erro, setErro]        = useState<string | null>(null);

  async function buscar() {
    const limpo = cep.replace(/\D/g, "");
    if (limpo.length !== 8) {
      setErro("CEP inválido — use 8 dígitos.");
      return;
    }

    setLoading(true);
    setErro(null);
    // Não limpa o resultado anterior durante a busca —
    // mantém o mapa visível enquanto o novo carrega

    try {
      const res  = await fetch(`${API_URL}/renda/cep/${limpo}`);
      const data = await res.json();

      if (!res.ok) {
        setErro(data.detail || "Erro na API.");
        // Limpa o resultado anterior só em caso de erro
        setResult(null);
        return;
      }

      if (!data.resumo) {
        setErro("Endereço fora da área mapeada.");
        setResult(null);
        return;
      }

      setResult(data);
    } catch {
      setErro("Não foi possível conectar à API.");
      setResult(null);
    } finally {
      setLoading(false);
    }
  }

  const mapPoint: MapPoint | null = resultado
    ? {
        latitude:  resultado.endereco.latitude,
        longitude: resultado.endereco.longitude,
        estado:    resultado.endereco.estado,
        geojson:   resultado.geojson,
        geometria: resultado.endereco.geometria,
      }
    : null;

  const temVariacao =
    resultado &&
    resultado.resumo.total_setores > 1 &&
    resultado.resumo.renda_media_minima !== resultado.resumo.renda_media_maxima;

  return (
    <>
      {/* ── NAV ── */}
      <nav style={{
        position: "sticky", top: 0, zIndex: 50,
        background: "rgba(245, 245, 245, 0.82)",
        backdropFilter: "saturate(180%) blur(20px)",
        WebkitBackdropFilter: "saturate(180%) blur(20px)",
        borderBottom: "1px solid rgba(0,0,0,0.06)",
        height: 52,
        display: "flex", alignItems: "center",
        padding: "0 40px",
        justifyContent: "space-between",
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <div style={{
            width: 28, height: 28, borderRadius: 8,
            background: "#000",
            display: "flex", alignItems: "center", justifyContent: "center",
          }}>
            <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
              <circle cx="7" cy="7" r="5.5" stroke="#00e5b0" strokeWidth="1.5" />
              <circle cx="7" cy="7" r="1.5" fill="#00e5b0" />
            </svg>
          </div>
          <span style={{ fontSize: 14, fontWeight: 500, color: "#1a1a1a", letterSpacing: "-0.2px" }}>
            GeoMarketing
          </span>
        </div>
        <span style={{ fontSize: 12, color: "rgba(26,26,26,0.4)", letterSpacing: "-0.1px" }}>
          Censo IBGE 2022
        </span>
      </nav>

      {/* ── HERO ── */}
      <section style={{
        background: "#000", color: "#fff",
        padding: "80px 40px 72px",
        textAlign: "center",
      }}>
        <p style={{
          fontSize: 12, fontWeight: 500, letterSpacing: "0.12em",
          color: "#00e5b0", textTransform: "uppercase", marginBottom: 20,
        }}>
          Inteligência Geoespacial para Negócios
        </p>
        <h1 style={{
          fontFamily: "var(--font-serif)",
          fontSize: "clamp(36px, 5vw, 56px)",
          fontWeight: 600, lineHeight: 1.07, letterSpacing: "-0.5px",
          color: "#fff", margin: "0 auto 20px", maxWidth: 640,
        }}>
          Escolha a localização certa para o seu negócio
        </h1>
        <p style={{
          fontSize: 17, fontWeight: 300, lineHeight: 1.55,
          color: "rgba(255,255,255,0.6)",
          maxWidth: 480, margin: "0 auto 40px", letterSpacing: "-0.2px",
        }}>
          Dados oficiais do Censo 2022 por setor censitário — perfil de renda,
          classe social e área geográfica exata.
        </p>

        {/* Busca */}
        <div style={{ display: "flex", gap: 10, maxWidth: 440, margin: "0 auto" }}>
          <input
            type="text"
            placeholder="CEP — ex: 22071-100"
            value={cep}
            onChange={(e) => setCep(formatarCEP(e.target.value))}
            onKeyDown={(e) => e.key === "Enter" && buscar()}
            maxLength={9}
            style={{
              flex: 1, height: 48, padding: "0 18px",
              borderRadius: 8, border: "none",
              background: "rgba(255,255,255,0.10)",
              color: "#fff", fontSize: 16,
              fontFamily: "var(--font-sans)", fontWeight: 400,
              letterSpacing: "-0.2px", outline: "none",
              transition: "background 0.15s",
            }}
            onFocus={(e) => (e.target.style.background = "rgba(255,255,255,0.15)")}
            onBlur={(e)  => (e.target.style.background = "rgba(255,255,255,0.10)")}
          />
          <button
            onClick={buscar}
            disabled={loading}
            style={{
              height: 48, padding: "0 24px", borderRadius: 8, border: "none",
              background: loading ? "#00c49a" : "#00e5b0",
              color: "#000", fontSize: 15, fontWeight: 600,
              fontFamily: "var(--font-sans)",
              cursor: loading ? "not-allowed" : "pointer",
              letterSpacing: "-0.1px",
              transition: "background 0.15s, transform 0.1s",
              flexShrink: 0,
            }}
            onMouseDown={(e) => (e.currentTarget.style.transform = "scale(0.97)")}
            onMouseUp={(e)   => (e.currentTarget.style.transform = "scale(1)")}
          >
            {loading ? "Buscando…" : "Buscar"}
          </button>
        </div>

        {erro && (
          <p style={{ marginTop: 16, fontSize: 13, color: "#e05c5c", letterSpacing: "-0.1px" }}>
            {erro}
          </p>
        )}
      </section>

      {/* ── CONTEÚDO — duas colunas ── */}
      <section style={{
        maxWidth: 1200, margin: "0 auto",
        padding: "56px 40px",
        display: "grid",
        gridTemplateColumns: resultado ? "380px 1fr" : "1fr",
        gap: 32,
        transition: "grid-template-columns 0.3s ease",
      }}>

        {/* Coluna esquerda */}
        {resultado && (
          <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>

            {/* Endereço */}
            <div style={{
              background: "#fff", borderRadius: 12,
              padding: "24px 28px",
              boxShadow: "rgba(0,0,0,0.07) 0px 2px 20px",
            }}>
              <p style={{
                fontSize: 11, fontWeight: 500, letterSpacing: "0.1em",
                color: "rgba(26,26,26,0.4)", textTransform: "uppercase", marginBottom: 10,
              }}>
                Endereço
              </p>
              <p style={{ fontSize: 16, fontWeight: 500, color: "#1a1a1a", marginBottom: 4, letterSpacing: "-0.2px" }}>
                {resultado.endereco.logradouro || "—"}
              </p>
              <p style={{ fontSize: 13, color: "rgba(26,26,26,0.5)", letterSpacing: "-0.1px" }}>
                {resultado.endereco.bairro} · {resultado.endereco.cidade} · {resultado.endereco.estado}
              </p>
            </div>

            {/* Perfil socioeconômico */}
            <div style={{
              background: "#000", borderRadius: 12,
              padding: "28px 28px",
              boxShadow: "rgba(0,0,0,0.18) 0px 4px 24px",
            }}>
              <p style={{
                fontSize: 11, fontWeight: 500, letterSpacing: "0.1em",
                color: "rgba(255,255,255,0.4)", textTransform: "uppercase", marginBottom: 16,
              }}>
                Perfil Socioeconômico
              </p>

              <div style={{ marginBottom: 20 }}>
                <p style={{ fontSize: 12, color: "rgba(255,255,255,0.4)", marginBottom: 6, letterSpacing: "-0.1px" }}>
                  Renda típica na via (Mediana)
                </p>
                <p style={{ fontSize: 32, fontWeight: 600, color: "#00e5b0", letterSpacing: "-0.8px", lineHeight: 1.1 }}>
                  {formatarRenda(resultado.resumo.renda_mediana)}
                </p>
                {temVariacao ? (
                  <p style={{ fontSize: 13, color: "rgba(255,255,255,0.5)", marginTop: 8, letterSpacing: "-0.1px" }}>
                    Variação: de {formatarRenda(resultado.resumo.renda_media_minima)} a{" "}
                    {formatarRenda(resultado.resumo.renda_media_maxima)}
                  </p>
                ) : (
                  <p style={{ fontSize: 13, color: "rgba(255,255,255,0.5)", marginTop: 8, letterSpacing: "-0.1px" }}>
                    por domicílio / mês
                  </p>
                )}
              </div>

              <div style={{
                display: "inline-flex", alignItems: "center", gap: 8,
                background: "rgba(255,255,255,0.07)",
                borderRadius: 980, padding: "6px 14px",
              }}>
                <div style={{
                  width: 8, height: 8, borderRadius: "50%",
                  background: CLASSE_COR[resultado.resumo.classe_predominante] || "#00e5b0",
                }} />
                <span style={{ fontSize: 13, fontWeight: 500, color: "#fff", letterSpacing: "-0.1px" }}>
                  Classe {resultado.resumo.classe_predominante}
                </span>
              </div>
            </div>

            {/* Setores */}
            {resultado.resumo.total_setores > 1 && (
              <div style={{
                background: "#fff", borderRadius: 12,
                padding: "20px 28px",
                boxShadow: "rgba(0,0,0,0.07) 0px 2px 20px",
              }}>
                <p style={{
                  fontSize: 11, fontWeight: 500, letterSpacing: "0.1em",
                  color: "rgba(26,26,26,0.4)", textTransform: "uppercase", marginBottom: 14,
                }}>
                  {resultado.resumo.total_setores} setores na área
                </p>
                <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
                  {resultado.setores.map((s) => (
                    <div key={s.setor_censitario} style={{
                      display: "flex", justifyContent: "space-between", alignItems: "center",
                    }}>
                      <span style={{ fontSize: 11, fontFamily: "monospace", color: "rgba(26,26,26,0.35)" }}>
                        {s.setor_censitario}
                      </span>
                      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                        <span style={{ fontSize: 13, fontWeight: 500, color: "#1a1a1a", letterSpacing: "-0.1px" }}>
                          {formatarRenda(s.renda_media)}
                        </span>
                        <span style={{
                          fontSize: 10, fontWeight: 600,
                          color: CLASSE_COR[s.classe_social_estimada] || "#00e5b0",
                          background: "rgba(0,229,176,0.1)",
                          padding: "2px 7px", borderRadius: 980,
                        }}>
                          {s.classe_social_estimada}
                        </span>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {resultado.fonte_geocoding === "cache" && (
              <p style={{ fontSize: 11, color: "rgba(26,26,26,0.25)", textAlign: "right", letterSpacing: "-0.1px" }}>
                resultado em cache
              </p>
            )}
          </div>
        )}

        {/* Mapa */}
        <div style={{
          borderRadius: 16, overflow: "hidden",
          boxShadow: "rgba(0,0,0,0.10) 0px 4px 32px",
          minHeight: resultado ? 520 : 420,
          background: "#e8e8e8",
        }}>
          <MapView resultado={mapPoint} />
        </div>
      </section>

      {/* Estado vazio */}
      {!resultado && !loading && (
        <div style={{ textAlign: "center", padding: "0 40px 80px" }}>
          <p style={{ fontSize: 14, color: "rgba(26,26,26,0.35)", letterSpacing: "-0.1px" }}>
            Digite um CEP acima para visualizar os dados do setor censitário no mapa
          </p>
        </div>
      )}

      {/* Footer */}
      <footer style={{
        borderTop: "1px solid rgba(0,0,0,0.06)",
        padding: "24px 40px",
        display: "flex", justifyContent: "space-between", alignItems: "center",
      }}>
        <span style={{ fontSize: 12, color: "rgba(26,26,26,0.35)", letterSpacing: "-0.1px" }}>
          © 2025 GeoMarketing
        </span>
        <span style={{ fontSize: 12, color: "rgba(26,26,26,0.35)", letterSpacing: "-0.1px" }}>
          Fonte: IBGE Censo 2022
        </span>
      </footer>
    </>
  );
}