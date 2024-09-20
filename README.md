# PlanetLabs imagery order and download tool
Tools and scripts to use the PlanetLabs API

Code is working with [Planet Python Client V1](https://developers.planet.com/docs/pythonclient/), probably largely deprecated in v2. The provided conda environment (`planet_env.yml`) should include all necessary packages. Make sure to have an environment variable `PL_API_KEY` holding your API key or pass it to the script directly.

- `search-and-order.py`  
  Places orders for a specific geojson area (see examples in `./filters`), giving an indication of used quota and splitting up in separate order chunks when there are too many assets for a single order.
- `download-order.py`  
  Downloads processed orders in bulk, matching part of string with the order name
- `create-daily-planet-composites.r`  
  First attempt at a mosaicking algorithm for scenes of a single day using R `terra`
