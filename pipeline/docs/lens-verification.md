# Lens Verification — Equity Flag Audit

| Neighbourhood | analysis_neighborhoods | ej_communities | equity_strategy | flag_count |
|---|---|---|---|---|
| Bayview Hunters Point | True | True | True | 3 |
| Bernal Heights | True | False | False | 1 |
| Castro/Upper Market | True | False | False | 1 |
| Excelsior | True | False | True | 2 |
| Financial District/South Beach | True | False | False | 1 |
| Haight Ashbury | True | False | False | 1 |
| Hayes Valley | True | False | True | 2 |
| Inner Richmond | True | False | False | 1 |
| Inner Sunset | True | False | False | 1 |
| Japantown | True | False | True | 2 |
| Lone Mountain/USF | True | False | False | 1 |
| Marina | True | False | False | 1 |
| Mission | True | True | True | 3 |
| Mission Bay | True | False | False | 1 |
| Nob Hill | True | False | False | 1 |
| Noe Valley | True | False | False | 1 |
| North Beach | True | True | False | 2 |
| Oceanview/Merced/Ingleside | True | False | True | 2 |
| Outer Mission | True | False | True | 2 |
| Outer Richmond | True | False | False | 1 |
| Pacific Heights | True | False | True | 2 |
| Portola | True | True | True | 3 |
| Potrero Hill | True | False | False | 1 |
| South of Market | True | True | False | 2 |
| Sunset/Parkside | True | False | False | 1 |
| Tenderloin | True | True | True | 3 |
| Twin Peaks | True | False | False | 1 |
| Visitacion Valley | True | True | True | 3 |
| West of Twin Peaks | True | False | False | 1 |
| Western Addition | True | False | True | 2 |

## Notes

- EJ Communities filtered to CalEnviroScreen score >= 21 (top 1/3 of cumulative burden).
- Equity Strategy polygons may not align exactly with Analysis Neighbourhood boundaries; edge-case addresses can cause a neighbourhood to inherit an equity flag from an adjacent polygon.
- In sample mode, per-neighbourhood counts are small; a full run gives more representative flags.
