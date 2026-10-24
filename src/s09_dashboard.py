r"""s09 · Member D · Task 3 + 4: map dashboard with live MQTT data.

One process = Dash web server + MQTT subscriber thread + DuckDB store:

    MQTT broker --> EventSubscriber (paho thread) --> LiveState (memory, current state)
                                                  \-> EventStore (DuckDB, history)
    Dash dcc.Interval (every ~1 s) --> read LiveState / EventStore --> update map + charts

Usage:
    python src/s09_dashboard.py      # then open http://127.0.0.1:8050
    python src/s05_publish_events.py # in another terminal
"""
from dash import Dash, Input, Output, dcc, html
import dash_leaflet as dl

from s08_event_store import EventStore
from s07_live_state import LiveState
from s06_subscriber import EventSubscriber

NSW_CENTER = [-33.0, 147.0]
REFRESH_MS = 1000

live_state = LiveState()
store = EventStore()
subscriber = EventSubscriber(live_state=live_state, store=store)

app = Dash(__name__, title="NSW EV Charging Live")

app.layout = html.Div([
    html.H2("NSW EV Charging - live prices & usage"),
    html.Div(id="metrics"),                     # current + historical metric cards (Task 4)
    html.Div([
        dl.Map(center=NSW_CENTER, zoom=6, style={"height": "70vh", "flex": "2"}, children=[
            dl.TileLayer(),
            dl.LayerGroup(id="markers"),
        ]),
        html.Div(id="detail-panel", style={"flex": "1"}),   # selected charger + live session curve
    ], style={"display": "flex", "gap": "12px"}),
    dcc.Store(id="selected-station"),
    dcc.Interval(id="tick", interval=REFRESH_MS),
    # Optional (not assessed): filters by operator / charger rating
])


@app.callback(Output("markers", "children"), Input("tick", "n_intervals"))
def update_markers(_):
    """TODO:
      - snapshot = live_state.snapshot(); only stations that have sent events get a marker
        ("dynamically add a marker on each received message")
      - position + static details from store.charger_details() (cache it, it does not change)
      - marker shows >= 1 dynamic attribute: e.g. colour by occupancy, label/tooltip with price
      - dl.Popup with name, address, plug types, operator network, latest price
      - give each marker id={"type": "marker", "station": sid} for the click callback
    """
    return []


@app.callback(Output("detail-panel", "children"),
              Input("tick", "n_intervals"), Input("selected-station", "data"))
def update_detail(_, station_id):
    """Full marks: live line chart of the active session on the clicked charger.

    TODO:
      - points = live_state.session_curve(station_id)
      - dcc.Graph with plotly line (x = event_ts, y = energy_kwh or power_kw / soc_pct)
      - show "no active session" when empty
    """
    return html.Div("Click a charger on the map.")


# TODO: callback Input({"type": "marker", "station": ALL}, "n_clicks") -> Output("selected-station", "data")
#       (use dash.ctx.triggered_id to find which marker was clicked)


@app.callback(Output("metrics", "children"), Input("tick", "n_intervals"))
def update_metrics(_):
    """TODO: >= 1 current metric (store.current_metrics()) and
             >= 1 historical metric (store.historical_metrics()); refresh less often
             than the map if queries are slow (separate dcc.Interval)."""
    return html.Div()


if __name__ == "__main__":
    subscriber.start()
    try:
        # debug=False: the reloader would start a second process -> second DuckDB writer
        app.run(debug=False)
    finally:
        subscriber.stop()
        store.close()
