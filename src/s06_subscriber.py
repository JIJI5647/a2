"""s06 · Member B · Task 3/4: MQTT subscriber; receives events, updates LiveState and EventStore.

Same pattern as the Week 9 tutorial (on_connect -> subscribe, on_message -> json.loads),
but it runs in a background thread (loop_start) so the dashboard can serve pages.

Can also run on its own for debugging (prints one line per message, no dashboard):
    python src/s06_subscriber.py
"""
from __future__ import annotations

import paho.mqtt.client as mqtt

import s00_config as config
import s01_messages as messages


def describe(event: dict) -> str:
    """One readable line per event for the console."""
    kind = event["event_type"] + (f"/{event['status']}" if "status" in event else "")
    if event["event_type"] == "price_update":
        detail = f"${event['price_per_kwh']:.2f}/kWh (was {event['prev_price_per_kwh']})"
    elif event["event_type"] == "meter":
        detail = f"{event['session_id']} {event['power_kw']} kW, {event['energy_kwh']} kWh, {event['soc_pct']}%"
    elif event.get("status") == "start":
        detail = f"{event['session_id']} connector {event['connector_id']}, {event['vehicle_model']}"
    else:
        detail = f"{event['session_id']} {event['energy_kwh']} kWh, ${event['cost_aud']}"
    return f"#{event.get('seq', '-'):>6} {event['event_ts']} {kind:<14} {event['station_id']:<7} {detail}"


class EventSubscriber:
    def __init__(self, live_state=None, store=None):
        self.live_state = live_state
        self.store = store
        self.received = 0
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        print(f"connected to {config.BROKER_HOST}: {reason_code}; subscribing {config.TOPIC_ROOT}/#",
              flush=True)
        client.subscribe(f"{config.TOPIC_ROOT}/#", qos=config.QOS)

    def _on_message(self, client, userdata, msg):
        # paho swallows exceptions raised in callbacks, so log them here.
        try:
            event = messages.decode(msg.payload)
            if event is None:          # cleared retained message
                return
            self.received += 1
            # TODO: drop duplicates by event_id; detect gaps/out-of-order via event["seq"]
            if self.live_state is not None:
                self.live_state.apply(event)
            if self.store is not None:
                self.store.insert_event(event)
            if self.live_state is None and self.store is None:
                print(describe(event), flush=True)
        except Exception as exc:
            print(f"skipped message on {msg.topic}: {exc!r}", flush=True)

    def start(self) -> None:
        self.client.connect(config.BROKER_HOST, config.BROKER_PORT, 60)
        self.client.loop_start()

    def stop(self) -> None:
        self.client.loop_stop()
        self.client.disconnect()


if __name__ == "__main__":
    sub = EventSubscriber()
    sub.client.connect(config.BROKER_HOST, config.BROKER_PORT, 60)
    try:
        sub.client.loop_forever()
    except KeyboardInterrupt:
        print(f"\nreceived {sub.received} messages")
