# Relâmpagos GLM (NOAA) → data/raios.json

Pipeline que roda no GitHub Actions a cada ~5 min:
1. Lista no bucket público da NOAA (`noaa-goes19`, GOES-East) os arquivos GLM L2 LCFA dos últimos 15 min.
2. Lê os flashes (lat, lon, horário, energia), filtra a caixa do Brasil + vizinhos e descarta flashes de qualidade ruim.
3. Grava `data/raios.json` no repositório. O site lê esse arquivo pela URL do `raw.githubusercontent.com`.

## Instalação (uma vez)
1. Copie `glm_pipeline.py` para `raios/glm_pipeline.py` na raiz do repositório.
2. Copie `.github/workflows/glm-raios.yml` para o mesmo caminho no repositório.
3. Em Settings → Actions → General, em "Workflow permissions", deixe **Read and write permissions** (o bot precisa dar push em `data/raios.json`).
4. Rode manualmente: Actions → "Relâmpagos GLM" → Run workflow.
5. Confira que apareceu `data/raios.json`. A URL pública fica `https://raw.githubusercontent.com/<dono>/<repo>/main/data/raios.json`.

## Formato do JSON
```
{ "generated": "2026-11-07T17:10:00Z", "window_min": 15, "count": 123,
  "fields": ["lat","lon","t","energy"],
  "flashes": [[-23.512, -46.633, 1762535400, 120], ...] }
```
`t` é epoch em segundos (UTC). `energy` está na unidade do GLM (J), sem conversão.

## Limitações conhecidas
- **Latência**: cron de 5 min + processamento + cache do raw do GitHub. Espere alguns minutos de atraso em relação ao relâmpago. Medir na prática.
- **Resolução**: GLM tem ~8 km no nadir e piora nas bordas. Mostra a região da tempestade, não o raio exato no chão.
- **Nuvem-solo vs. intranuvem**: o GLM detecta os dois. Não separa sem dados extras.
- **Horário do flash**: o código usa o início da varredura do arquivo mais `flash_time_offset_of_first_event`. A unidade desse campo (s ou ms) **precisa ser confirmada** na primeira execução real. Se estiver em ms, ajustar `read_granule`.
- **Nomes dos campos** (`flash_lat`, `flash_lon`, `flash_energy`, `flash_quality_flag`): conferidos só pela documentação do produto, não contra um arquivo real. A primeira execução mostra qualquer erro no log.
- **Sem o GitHub Actions**: se o cron atrasar, o arquivo fica velho. O site deve mostrar a idade do dado (`generated`).

## Teste local
`python3 raios/test_glm.py` (lógica pura, sem rede). Rodar a pipeline de verdade exige rede para a NOAA.
