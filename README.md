# GeoMarketing

API e frontend para consulta socioeconômica por CEP, com foco no MVP:
receber um CEP, geocodificar o endereço e estimar perfil de renda na área da via.

## Escopo do MVP

- Entrada: CEP brasileiro com 8 dígitos.
- Saída principal: renda mediana estimada na área da rua do CEP.
- Cobertura operacional: estados do Rio de Janeiro (RJ) e São Paulo (SP).

## Contrato da métrica de renda (MVP)

A métrica atual de `renda_mediana` é definida como:

- A rua geocodificada (LineString/Polygon/Point) recebe um buffer em metros.
- São coletados os setores censitários que intersectam esse buffer.
- Para cada setor é usada a coluna `renda_media` (IBGE já agregada por setor).
- A resposta final retorna a mediana desses valores setoriais.

Em termos formais:

`renda_mediana = mediana({ renda_media_setor_i | setor_i intersecta buffer(geometria_cep) })`

## Limitações conhecidas desta métrica

- Não é mediana domiciliar individual por endereço.
- É uma aproximação espacial baseada em setores censitários agregados.
- Pode variar com ambiguidade de geocoding quando múltiplas geometrias são possíveis.

## Próximos passos planejados

- Inclusão de POIs próximos (escolas, hospitais, supermercados, segurança etc.).
- Ranking multicritério de localização com pesos por categoria.