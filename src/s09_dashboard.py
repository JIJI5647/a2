"""Task 3/4: Member D live MQTT map, active-session curves and DuckDB metrics.

Run ``python src/s09_dashboard.py`` and open http://127.0.0.1:8050.
Run s05 in another terminal with the same A2_TOPIC_ROOT, after MQTT connects.
One process owns the subscriber and store; Dash's reloader must stay disabled.
Importing this module does not open a database or start a network connection.
"""
from datetime import datetime
import math

from dash import ALL, Dash, Input, Output, Patch, State, ctx, dcc, html, no_update
from dash.exceptions import PreventUpdate
import dash_leaflet as dl
import plotly.graph_objects as go
from plotly.subplots import make_subplots

import s00_config as config
from s08_event_store import EventStore
from s07_live_state import LiveState
from s06_subscriber import EventSubscriber

NSW_CENTER = [-33.0, 147.0]
REFRESH_MS = 1000
METRICS_REFRESH_MS = 5000
GREEN, AMBER, RED, BLUE = "#16836b", "#d48a12", "#d14848", "#2563eb"
PANEL = {"background": "white", "border": "1px solid #dce4eb",
         "borderRadius": "14px", "padding": "20px"}
MUTED = {"color": "#62748a", "fontSize": "13px", "lineHeight": "1.6"}


def format_number(value, digits=0, prefix="", suffix=""):
    """Keep unknown values distinct from real zeroes."""
    if value is None:
        return "Unknown"
    return f"{prefix}{value:,.{digits}f}{suffix}"


def format_time(value):
    if value is None:
        return "Not received"
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    return value.strftime("%d %b %Y %H:%M:%S")


def station_position(details):
    try:
        lat, lon = float(details["lat"]), float(details["lon"])
    except (KeyError, TypeError, ValueError):
        return None
    if math.isfinite(lat) and math.isfinite(lon) and -90 <= lat <= 90 and -180 <= lon <= 180:
        return [lat, lon]
    return None


def station_fields(details, station):
    """Shared static/dynamic details for both the popup and side panel."""
    return html.Dl([
        html.Div([html.Dt(label, style={"fontWeight": "600"}),
                  html.Dd(value, style={"margin": "2px 0 10px", "overflowWrap": "anywhere"})])
        for label, value in [
            ("Station ID", details["station_id"]),
            ("Address", details.get("address") or "Unknown"),
            ("Operator network", details.get("operator") or "Unknown"),
            ("Plug types", details.get("plug_types") or "Unknown"),
            ("Charger rating", details.get("charger_rating") or "Unknown"),
            ("Latest received price", format_number(station.get("price_per_kwh"), 2, "$", "/kWh")),
            ("Price event time", format_time(station.get("price_ts"))),
            ("Observed occupied connectors",
             f"{station['occupied_connectors']} / {details['connectors']}"),
        ]
    ], style={"fontSize": "13px", "marginBottom": "0"})


def marker_content(details, station):
    price = format_number(station.get("price_per_kwh"), 2, "$", "/kWh")
    occupied = station["occupied_connectors"]
    total = details["connectors"]
    color = RED if occupied >= total else AMBER if occupied else GREEN
    tooltip = f"{details['name']} | {price} | occupied {occupied}/{total}"
    popup = html.Div([html.Strong(details["name"]), station_fields(details, station)])
    return color, tooltip, popup


def session_figure(station_id, sessions):
    """Plot every active connector from one coherent LiveState snapshot."""
    figure = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.15,
                           subplot_titles=("Cumulative energy", "Charging power"))
    colors = [BLUE, GREEN, AMBER, "#8b5cf6", "#db2777", "#0891b2"]
    for index, (session_id, session) in enumerate(sorted(sessions.items())):
        readings = sorted(session["meter_readings"].values(),
                          key=lambda point: (datetime.fromisoformat(point["event_ts"]), point["seq"]))
        if not readings:
            continue
        times = [point["event_ts"] for point in readings]
        color = colors[index % len(colors)]
        name = f"Connector {session['connector_id']} · {session.get('vehicle_model') or 'Unknown vehicle'}"
        for row, field, unit in [(1, "energy_kwh", "kWh"), (2, "power_kw", "kW")]:
            figure.add_trace(go.Scatter(
                x=times, y=[point[field] for point in readings], mode="lines+markers",
                name=name, legendgroup=session_id, showlegend=row == 1,
                line={"color": color, "width": 2}, marker={"size": 5},
                hovertemplate=f"%{{x}}<br>%{{y:.2f}} {unit}<extra>%{{fullData.name}}</extra>",
            ), row=row, col=1)
    figure.update_yaxes(title_text="kWh", row=1, col=1, rangemode="tozero")
    figure.update_yaxes(title_text="kW", row=2, col=1, rangemode="tozero")
    figure.update_xaxes(type="date", title_text="Event time (generator local time)", row=2, col=1)
    figure.update_layout(template="plotly_white", height=440, margin={"l": 45, "r": 15, "t": 35, "b": 30},
                         legend={"orientation": "h", "y": -0.22},
                         hovermode="x unified", uirevision=station_id)
    return figure


def render_detail(station_id, snapshot, charger_details):
    if not station_id:
        return [html.H3("Explore a charger"), html.P(
            "Click a station on the map to see its price, connector occupancy and live charging curves.",
            style=MUTED)]
    details = charger_details.get(station_id)
    if details is None:
        return [html.P("Station details are unavailable.")]
    station = snapshot.get(station_id)
    if station is None:
        return [html.H3(details["name"]), html.P("Waiting for this station's first event.", style=MUTED)]

    children = [html.H3(details["name"], style={"marginTop": "0"}), station_fields(details, station),
                html.Hr(style={"border": "0", "borderTop": "1px solid #dce4eb"}),
                html.H4("Active charging sessions")]
    sessions = station["active_sessions"]
    if not sessions:
        children.append(html.P("No active session observed at this station.", style=MUTED))
        return children

    for session_id, session in sorted(sessions.items()):
        meter = session["latest_meter"]
        children.append(html.Div([
            html.Strong(f"Connector {session['connector_id']} · {session.get('vehicle_model') or 'Unknown vehicle'}"),
            html.Div(session_id, style={**MUTED, "overflowWrap": "anywhere"}),
            html.Div(f"Plugged in: {format_time(session['start_ts'])}", style=MUTED),
            html.Div(
                (f"{format_number(meter['energy_kwh'], 2, suffix=' kWh')} · "
                 f"{format_number(meter['power_kw'], 1, suffix=' kW')} · "
                 f"SOC {format_number(meter['soc_pct'], 1, suffix='%')}")
                if meter else "Waiting for the first meter reading.", style=MUTED),
        ], style={"padding": "10px 0"}))
    if any(session["meter_readings"] for session in sessions.values()):
        children.append(dcc.Graph(id="session-chart", figure=session_figure(station_id, sessions),
                                  config={"displaylogo": False, "responsive": True}))
    return children


def metric_card(label, value, source):
    return html.Div([
        html.Div(label, style=MUTED),
        html.Div(value, style={"fontSize": "26px", "fontWeight": "700", "margin": "6px 0"}),
        html.Div(source, style={**MUTED, "fontSize": "11px"}),
    ], style={**PANEL, "padding": "14px 18px"})


def create_app(store, live_state, subscriber):
    """Accept shared services so local/offline checks need no MQTT connection."""
    charger_details = store.charger_details()  # Static A1 mapping: load once.
    app = Dash(__name__, title="NSW EV Charging Live", suppress_callback_exceptions=True)
    # Keep responsive styling in this file; no assets or dependency changes needed.
    app.index_string = app.index_string.replace("</head>", """<style>
        body { margin: 0; background: #f3f6f9; }
        .dashboard-metrics { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr));
                             gap: 12px; margin-bottom: 12px; }
        @media (max-width: 900px) {
            .dashboard-metrics { grid-template-columns: repeat(2, minmax(0, 1fr)); }
        }
        @media (max-width: 480px) {
            .dashboard-metrics { grid-template-columns: minmax(0, 1fr); }
        }
    </style></head>""")
    app.layout = html.Div([
        html.Div([
            html.Div("COMP5339 / EV CHARGING", style={**MUTED, "letterSpacing": "2px"}),
            html.H1("NSW charging live", style={"margin": "8px 0"}),
            html.P("Follow station prices, connector occupancy and charging activity.", style=MUTED),
            html.Div(id="connection-status", role="status", style=MUTED),
        ], style={"marginBottom": "22px"}),
        html.Div(id="metrics", className="dashboard-metrics"),
        html.Div(id="metrics-note", style={**MUTED, "marginBottom": "20px"}),
        html.Div([
            html.Div([
                html.H3("Station activity", style={"marginTop": "0"}),
                html.Div([
                    html.Span(label, style={"color": color, "marginRight": "16px"})
                    for label, color in [("● No observed active session", GREEN),
                                         ("● Partly occupied", AMBER), ("● Fully occupied", RED)]
                ], style={"fontSize": "12px", "marginBottom": "12px"}),
                dl.Map(id="station-map", center=NSW_CENTER, zoom=6,
                       style={"height": "65vh", "minHeight": "420px", "borderRadius": "10px"},
                       children=[dl.TileLayer(), dl.LayerGroup(id="markers"), dl.LayerGroup(id="selection-ring")]),
                html.P(id="map-status", role="status", style=MUTED),
                html.P("The map and curves show events received in this run. Connector occupancy lasts until unplug.",
                       style=MUTED),
            ], style={**PANEL, "flex": "2 1 550px", "minWidth": "0"}),
            html.Div(id="detail-panel", children=render_detail(None, {}, charger_details),
                     style={**PANEL, "flex": "1 1 380px", "minWidth": "0"}),
        ], style={"display": "flex", "flexWrap": "wrap", "gap": "18px", "alignItems": "flex-start"}),
        dcc.Store(id="selected-station"),
        dcc.Store(id="marker-stations", data=[]),
        dcc.Store(id="marker-values", data={}),
        dcc.Interval(id="tick", interval=REFRESH_MS, n_intervals=0),
        dcc.Interval(id="metrics-tick", interval=METRICS_REFRESH_MS, n_intervals=0),
    ], style={"fontFamily": "Segoe UI, Arial, sans-serif", "color": "#182b40", "background": "#f3f6f9",
              "minHeight": "100vh", "padding": "24px", "maxWidth": "1600px", "margin": "0 auto"})

    @app.callback(
        Output("markers", "children"), Output("marker-stations", "data"),
        Output("marker-values", "data"), Output("map-status", "children"),
        Input("tick", "n_intervals"), State("marker-stations", "data"), State("marker-values", "data"),
    )
    def update_markers(_, known_stations, previous_values):
        snapshot = live_state.snapshot()
        visible = {sid: station for sid, station in snapshot.items()
                   if sid in charger_details and station_position(charger_details[sid]) is not None}
        known_stations = list(known_stations or [])
        previous_values = previous_values or {}
        values = {sid: [station.get("price_per_kwh"), station.get("price_ts"), station["occupied_connectors"]]
                  for sid, station in visible.items()}
        patch = Patch()
        changed = False
        # Skip unchanged layers to preserve open popups during idle/meter refreshes.
        # Patches retain n_clicks, so periodic updates cannot become fake clicks.
        # Only append new stations; LiveState retains stations for the whole run.
        for index, sid in enumerate(known_stations):
            station = visible.get(sid)
            if station is None or values[sid] == previous_values.get(sid):
                continue
            color, tooltip, popup = marker_content(charger_details[sid], station)
            props = patch[index]["props"]
            props["color"] = color
            props["fillColor"] = color
            props["children"][0]["props"]["children"] = tooltip
            props["children"][1]["props"]["children"] = popup
            changed = True
        for sid in sorted(visible.keys() - set(known_stations)):
            details = charger_details[sid]
            color, tooltip, popup = marker_content(details, visible[sid])
            patch.append(dl.CircleMarker(
                id={"type": "marker", "station": sid}, center=station_position(details),
                radius=7, weight=3, color=color, fillColor=color, fillOpacity=0.85,
                n_clicks=0, bubblingMouseEvents=False,
                children=[dl.Tooltip(tooltip, id={"type": "station-tooltip", "station": sid}),
                          dl.Popup(popup, id={"type": "station-popup", "station": sid}, maxWidth=330)],
            ))
            known_stations.append(sid)
            changed = True
        status = (f"{len(visible)} stations observed in this run. Click a marker for details."
                  if visible else "Waiting for station events. Start the publisher after MQTT connects.")
        skipped = len(snapshot) - len(visible)
        if skipped:
            status += f" {skipped} stations lack mapped coordinates."
        return patch if changed else no_update, known_stations, values, status

    @app.callback(Output("selected-station", "data"), Output("selection-ring", "children"),
                  Input({"type": "marker", "station": ALL}, "n_clicks"),
                  State({"type": "marker", "station": ALL}, "id"), prevent_initial_call=True)
    def select_station(clicks, marker_ids):
        triggered = ctx.triggered_id
        if not isinstance(triggered, dict):
            raise PreventUpdate
        # New layers have n_clicks=0; adding one must not change the selection.
        for marker_id, count in zip(marker_ids, clicks):
            if marker_id == triggered and count:
                sid = triggered["station"]
                position = station_position(charger_details.get(sid, {}))
                if position is None:
                    raise PreventUpdate
                ring = dl.CircleMarker(center=position, radius=12, color=BLUE,
                                       weight=3, fill=False, interactive=False)
                return sid, [ring]
        raise PreventUpdate

    @app.callback(Output("detail-panel", "children"),
                  Input("tick", "n_intervals"), Input("selected-station", "data"))
    def update_detail(_, station_id):
        return render_detail(station_id, live_state.snapshot(), charger_details)

    @app.callback(Output("metrics", "children"), Output("metrics-note", "children"),
                  Input("metrics-tick", "n_intervals"))
    def update_metrics(_):
        current = store.current_metrics()
        history = store.historical_metrics()
        cards = [
            metric_card("Active sessions", format_number(current["active_sessions"]), "Latest stored state"),
            metric_card("Occupied connectors", format_number(current["occupied_connectors"]), "Latest stored state"),
            metric_card("Stations with prices", format_number(current["priced_stations"]), "Latest stored state"),
            metric_card("Completed sessions", format_number(history["completed_sessions"]), "Database history"),
            metric_card("Delivered energy", format_number(history["energy_kwh"], 1, suffix=" kWh"), "Completed sessions"),
            metric_card("Revenue", format_number(history["revenue_aud"], 2, "$"), "Completed sessions · AUD"),
            metric_card("Avg. connector occupancy", format_number(history["avg_occupied_minutes"], 1, suffix=" min"),
                        "Completed sessions with known starts"),
            metric_card("Avg. charging duration", format_number(history["avg_charge_minutes"], 1, suffix=" min"),
                        "Completed sessions"),
        ]
        note = (f"Latest stored event: {format_time(current['latest_event_ts'])} (generator local time). "
                f"{history['total_events']:,} stored events; {history['sessions_missing_start']:,} completed sessions "
                "have unknown starts. Metrics refresh every 5 seconds and include earlier runs in this database; "
                "current state means the latest observed stream state.")
        return cards, note

    @app.callback(Output("connection-status", "children"), Input("tick", "n_intervals"))
    def update_connection(_):
        connected = subscriber.client.is_connected()
        return [html.Span("● MQTT connected" if connected else "● MQTT disconnected / reconnecting",
                          style={"color": GREEN if connected else AMBER, "fontWeight": "600"}),
                html.Span(f" · This run: {subscriber.processed:,} processed · "
                          f"{subscriber.duplicates:,} duplicates · {subscriber.errors:,} errors")]

    return app


def main():
    with EventStore() as store:
        live_state = LiveState()
        subscriber = EventSubscriber(live_state=live_state, store=store)
        app = create_app(store, live_state, subscriber)
        try:
            # Async connection/retries let the UI open even while the broker is unavailable.
            subscriber.client.reconnect_delay_set(min_delay=1, max_delay=30)
            subscriber.client.connect_async(config.BROKER_HOST, config.BROKER_PORT, 60)
            subscriber.client.loop_start()
            app.run(host="127.0.0.1", port=8050, debug=False, use_reloader=False)
        finally:
            subscriber.stop()


if __name__ == "__main__":
    main()
