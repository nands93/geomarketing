"use client";

import { useState } from "react";
import dynamic from "next/dynamic";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import { Search, MapPin, TrendingUp, AlertCircle, Loader2 } from "lucide-react";

// MapLibre precisa rodar só no cliente (sem SSR)
const MapView = dynamic(() => import("@/components/MapView"), { ssr: false });

const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

interface Endereco {
  logradouro: string;
  bairro: string;
  cidade: string;
  latitude: number;
  longitude: number;
}

interface DadosCensitarios {
  setor_censitario: string;
  renda_media: number;
  classe_social_estimada: string;
}

interface ResultadoCEP {
  cep: string;
  endereco: Endereco;
  dados_censitarios: DadosCensitarios;
  fonte_geocoding: string;
}

const COR_CLASSE: Record<string, string> = {
  A: "bg-emerald-500/20 text-emerald-300 border-emerald-500/30",
  B: "bg-blue-500/20 text-blue-300 border-blue-500/30",
  C: "bg-yellow-500/20 text-yellow-300 border-yellow-500/30",
  "D/E": "bg-red-500/20 text-red-300 border-red-500/30",
};

function formatarRenda(valor: number): string {
  return valor.toLocaleString("pt-BR", { style: "currency", currency: "BRL" });
}

export default function Home() {
  const [cep, setCep] = useState("");
  const [loading, setLoading] = useState(false);
  const [resultado, setResultado] = useState<ResultadoCEP | null>(null);
  const [erro, setErro] = useState<string | null>(null);

  async function buscarCEP() {
    const cepLimpo = cep.replace(/\D/g, "");
    if (cepLimpo.length !== 8) {
      setErro("Digite um CEP válido com 8 dígitos.");
      return;
    }

    setLoading(true);
    setErro(null);
    setResultado(null);

    try {
      const res = await fetch(`${API_URL}/renda/cep/${cepLimpo}`);
      const data = await res.json();

      if (!res.ok) {
        setErro(data.detail || "Erro ao consultar a API.");
        return;
      }

      if (!data.dados_censitarios) {
        setErro("Endereço fora da área mapeada ou sem dados censitários.");
        return;
      }

      setResultado(data);
    } catch {
      setErro("Não foi possível conectar à API. Verifique se ela está rodando.");
    } finally {
      setLoading(false);
    }
  }

  function handleKeyDown(e: React.KeyboardEvent) {
    if (e.key === "Enter") buscarCEP();
  }

  const mapPoint = resultado
    ? {
        latitude: resultado.endereco.latitude,
        longitude: resultado.endereco.longitude,
        setor_censitario: resultado.dados_censitarios.setor_censitario,
      }
    : null;

  return (
    <main className="min-h-screen bg-[#0a0a0f] text-white font-sans">

      {/* Header */}
      <header className="border-b border-white/5 px-6 py-4 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="w-7 h-7 rounded bg-[#00e5b0] flex items-center justify-center">
            <MapPin className="w-4 h-4 text-black" />
          </div>
          <span className="font-semibold tracking-tight text-sm">GeoMarketing RJ</span>
        </div>
        <span className="text-xs text-white/30">Censo IBGE 2022 · Município do Rio de Janeiro</span>
      </header>

      <div className="max-w-6xl mx-auto px-6 py-10 grid grid-cols-1 lg:grid-cols-2 gap-8">

        {/* Coluna esquerda — busca + resultado */}
        <div className="flex flex-col gap-6">

          {/* Título */}
          <div>
            <h1 className="text-3xl font-bold tracking-tight leading-tight">
              Inteligência de<br />
              <span className="text-[#00e5b0]">localização</span> para<br />
              o seu negócio
            </h1>
            <p className="mt-3 text-sm text-white/50 leading-relaxed">
              Digite um CEP e descubra o perfil socioeconômico do setor
              censitário — baseado nos dados oficiais do Censo 2022.
            </p>
          </div>

          {/* Campo de busca */}
          <div className="flex gap-2">
            <Input
              placeholder="Ex: 22071-100"
              value={cep}
              onChange={(e) => setCep(e.target.value)}
              onKeyDown={handleKeyDown}
              maxLength={9}
              className="bg-white/5 border-white/10 text-white placeholder:text-white/30 focus:border-[#00e5b0]/50 focus:ring-[#00e5b0]/20 h-11"
            />
            <Button
              onClick={buscarCEP}
              disabled={loading}
              className="bg-[#00e5b0] hover:bg-[#00c99a] text-black font-semibold h-11 px-5 shrink-0"
            >
              {loading ? (
                <Loader2 className="w-4 h-4 animate-spin" />
              ) : (
                <Search className="w-4 h-4" />
              )}
            </Button>
          </div>

          {/* Erro */}
          {erro && (
            <div className="flex items-start gap-3 p-4 rounded-lg bg-red-500/10 border border-red-500/20 text-red-300 text-sm">
              <AlertCircle className="w-4 h-4 mt-0.5 shrink-0" />
              {erro}
            </div>
          )}

          {/* Resultado */}
          {resultado && (
            <div className="rounded-xl border border-white/10 bg-white/5 overflow-hidden">

              {/* Endereço */}
              <div className="px-5 py-4">
                <div className="flex items-center justify-between mb-1">
                  <p className="text-xs text-white/40 uppercase tracking-widest">Endereço</p>
                  {resultado.fonte_geocoding === "cache" && (
                    <span className="text-[10px] text-white/20">cache</span>
                  )}
                </div>
                <p className="font-medium text-sm">
                  {resultado.endereco.logradouro || "—"}
                </p>
                <p className="text-xs text-white/50 mt-0.5">
                  {resultado.endereco.bairro} · {resultado.endereco.cidade}
                </p>
              </div>

              <Separator className="bg-white/5" />

              {/* Dados censitários */}
              <div className="px-5 py-4 grid grid-cols-2 gap-4">

                <div>
                  <p className="text-xs text-white/40 uppercase tracking-widest mb-2">
                    Renda média
                  </p>
                  <div className="flex items-end gap-2">
                    <TrendingUp className="w-4 h-4 text-[#00e5b0] mb-0.5" />
                    <span className="text-xl font-bold text-[#00e5b0]">
                      {formatarRenda(resultado.dados_censitarios.renda_media)}
                    </span>
                  </div>
                  <p className="text-[11px] text-white/30 mt-1">por domicílio / mês</p>
                </div>

                <div>
                  <p className="text-xs text-white/40 uppercase tracking-widest mb-2">
                    Classe estimada
                  </p>
                  <Badge
                    className={`text-sm font-bold px-3 py-1 border ${
                      COR_CLASSE[resultado.dados_censitarios.classe_social_estimada] ||
                      "bg-white/10 text-white"
                    }`}
                  >
                    Classe {resultado.dados_censitarios.classe_social_estimada}
                  </Badge>
                </div>
              </div>

              <Separator className="bg-white/5" />

              {/* Setor censitário */}
              <div className="px-5 py-3">
                <p className="text-[11px] text-white/30">
                  Setor censitário:{" "}
                  <span className="font-mono text-white/50">
                    {resultado.dados_censitarios.setor_censitario}
                  </span>
                </p>
              </div>
            </div>
          )}

          {/* Estado vazio */}
          {!resultado && !erro && !loading && (
            <div className="rounded-xl border border-dashed border-white/10 p-8 text-center">
              <MapPin className="w-8 h-8 text-white/20 mx-auto mb-3" />
              <p className="text-sm text-white/30">
                Digite um CEP para ver os dados do setor censitário
              </p>
            </div>
          )}
        </div>

        {/* Coluna direita — mapa */}
        <div className="rounded-xl overflow-hidden border border-white/10" style={{ minHeight: "500px" }}>
          <MapView resultado={mapPoint} />
        </div>
      </div>
    </main>
  );
}