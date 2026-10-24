# Synthetic NSW EV charging time series (COMP5339 Assignment 2)

Calibrated synthetic data for EV fast chargers: hourly **dynamic prices**, **charging sessions**
(start/end/duration/kWh/power), and **per-minute meter telemetry** (voltage, current, kW, kWh, SoC).

## Quick start
```bash
python generate_dataset.py --stations demo_stations.csv --start 2026-10-01 --days 14 --out data
```
The generator uses only the Python standard library. It downloads real AEMO NSW wholesale prices 
for the chosen months and caches them in `data/aemo_cache/`.

## Outputs (`data/`)
| File | Content |
|---|---|
| `stations.csv` | id, name, lat, lon, site\_type (urban/regional), connectors, max\_kw, base\_price |
| `prices_hourly.csv` | ts, station\_id, price\_per\_kwh, wholesale\_per\_kwh |
| `sessions.csv` | session\_id, station, connector, vehicle\_model, start\_ts, charge\_end\_ts, end\_ts, charge\_minutes, energy\_kwh, avg\_kw, peak\_kw, price\_per\_kwh, cost\_aud |
| `session_metering.csv` | time-ordered meter telemetry data: ts, station\_id, connector\_id, session\_id, volatge\_v, current\_a, power\_kw, energy\_kwh, soc\_pct |

## Using real NRMA charger locations
`demo_stations.csv` uses **town-centre coordinates**, not real charger sites. To use real sites,
supply a CSV with the same columns from the ev-charger dataset already used in Assignment 1.

## How it is modelled and calibrated
**Prices.** NRMA does **not** use time-of-use or dynamic pricing. It charges a flat $/kWh by charger
power (about $0.54–0.60 up to 150 kW, $0.59–0.65 at 175 kW and above, $0.41–0.45 AC; members get about
10% off). Since April 2025 it has also used *location-based* prices, up to about $0.79/kWh.
The generator therefore models a **hypothetical dynamic tariff**:
`price = station_base + pass_through × (hourly AEMO NSW1 RRP − period mean) + $0.10 (4–9pm)`,
clipped to $0.20–1.20/kWh. This range matches real Australian time-of-use schemes: the Chargefox SA
trial charged $0.12–1.20/kWh, and Tesla Superchargers range from about $0.43 to $0.69. Use `--static-prices` for NRMA-style flat prices.

**Sessions.** Starts follow a non-homogeneous Poisson process per connector:
- **Hourly shape:** taken from the ARENA charts. Starts are mostly mid-day, and urban sites get a weekday 6–10pm bump.
- **Volume:** 4 sessions per connector per day at urban sites and 3 at regional sites (configurable).
- **Price response:** demand changes with the price ratio, with elasticity −0.5.
- **Busy connectors:** a driver who arrives at an occupied connector goes elsewhere.

**Charging physics.** Each session uses one vehicle from an approximate Australian fleet mix
(battery size, maximum DC kW, 400 V or 800 V architecture). It arrives at 20–55% state of charge and
targets about 74%. Power is limited by the charger's rating, the charger's current limit, the vehicle's
taper curve above 50% SoC, and a real-world derating factor. Pack voltage rises with SoC, and current = P/V.

**Calibration check** (14 days, 20 stations, seed 5339) against the ARENA 2023 averages:

| | generated | ARENA 2023 |
|---|---|---|
| urban | 26.0 min / 25.8 kWh | 27.3 min / 27.0 kWh |
| regional | 24.8 min / 25.2 kWh | 25.9 min / 24.5 kWh |

Caveat: the hourly start profile, the fleet mix and the derating are approximations read from charts
or estimated. They are not published figures.

## Real (non-synthetic) alternatives
- **Caltech ACN-Data**: real sessions with charging-current and pilot-signal time series. These are
  US Level-2 AC chargers, not DC fast chargers. Free API token; Python client `acnportal`. https://ev.caltech.edu/dataset
- **Perth & Kinross Council (Scotland)**: session-level data (start, end, kWh). IEEE DataPort, DOI 10.21227/3dbt-2v65
- **AEMO price & demand**: real 5-minute NSW wholesale prices, used here. https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/data-nem/aggregated-data

## Sources
- ARENA / Energeia, *Insights from Ultra-Fast Charging Network Data (Update)*, Aug 2023 (CC BY 3.0 AU): https://arena.gov.au/knowledge-bank/insights-from-ultra-fast-charging-network-data
- NRMA EV charging prices: https://mynrma.com.au/ev ; location-based pricing: https://zecar.com/reviews/nrma-ev-charging-network-introduces-tap-n-pay-and-location-based-pricing-at-some-locations
- Chargefox time-of-use trial: https://4x4australia.com.au/news/chargefox-trials-wild-fluctuating-ev-charging-prices
