# Lens Verification — Equity Flag Audit

| Neighbourhood | analysis_neighborhoods | ej_communities | equity_strategy | flag_count |
|---|---|---|---|---|
| Bayview Hunters Point | True | True | True | 3 |
| Bernal Heights | True | False | False | 1 |
| Castro/Upper Market | True | False | False | 1 |
| Chinatown | True | True | True | 3 |
| Excelsior | True | True | True | 3 |
| Financial District/South Beach | True | True | True | 3 |
| Glen Park | True | False | False | 1 |
| Golden Gate Park | True | True | False | 2 |
| Haight Ashbury | True | False | False | 1 |
| Hayes Valley | True | True | True | 3 |
| Inner Richmond | True | False | False | 1 |
| Inner Sunset | True | False | False | 1 |
| Japantown | True | False | True | 2 |
| Lakeshore | True | True | False | 2 |
| Lincoln Park | True | True | False | 2 |
| Lone Mountain/USF | True | False | False | 1 |
| Marina | True | False | False | 1 |
| McLaren Park | True | True | True | 3 |
| Mission | True | True | True | 3 |
| Mission Bay | True | True | False | 2 |
| Nob Hill | True | True | True | 3 |
| Noe Valley | True | False | False | 1 |
| North Beach | True | True | False | 2 |
| Oceanview/Merced/Ingleside | True | True | True | 3 |
| Outer Mission | True | True | True | 3 |
| Outer Richmond | True | True | False | 2 |
| Pacific Heights | True | False | True | 2 |
| Portola | True | True | True | 3 |
| Potrero Hill | True | True | False | 2 |
| Presidio | True | False | False | 1 |
| Presidio Heights | True | False | False | 1 |
| Russian Hill | True | True | False | 2 |
| Seacliff | True | False | False | 1 |
| South of Market | True | True | True | 3 |
| Sunset/Parkside | True | False | False | 1 |
| Tenderloin | True | True | True | 3 |
| Treasure Island | True | True | False | 2 |
| Twin Peaks | True | False | False | 1 |
| Visitacion Valley | True | True | True | 3 |
| West of Twin Peaks | True | False | False | 1 |
| Western Addition | True | True | True | 3 |

## Notes

- EJ Communities filtered to CalEnviroScreen score >= 21 (top 1/3 of cumulative burden).
- Equity Strategy polygons may not align exactly with Analysis Neighbourhood boundaries; edge-case addresses can cause a neighbourhood to inherit an equity flag from an adjacent polygon.
- In sample mode, per-neighbourhood counts are small; a full run gives more representative flags.
