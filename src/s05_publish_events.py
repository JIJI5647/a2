"""s05 · Member A · Task 2: publish the ordered event stream to MQTT.

Follows the Week 9 tutorial publisher (paho-mqtt, loop_start, publish, sleep),
reading data/events/event_stream.jsonl in order.

Usage:
    python src/s05_publish_events.py                 # dev: first config.DEV_MAX_EVENTS events
    python src/s05_publish_events.py --all           # full stream
    python src/s05_publish_events.py [--delay 0.1] [--start-seq 0] [--max-events N] [--no-catch-up]

With --start-seq N > 0 the publisher first re-sends the state at N (catch-up):
the latest price of each station and the start of every session still running.
"""
import argparse
import itertools
import json
import time

import paho.mqtt.client as mqtt

import s00_config as config
import s01_messages as messages


def connect() -> mqtt.Client:
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.connect(config.BROKER_HOST, config.BROKER_PORT, 60)
    client.loop_start()
    return client


def iter_events(start_seq: int, max_events: int | None):
    """Yield events from the JSONL stream, starting at start_seq (resume support)."""
    with open(config.EVENT_STREAM_PATH) as f:
        sent = 0
        for line in f:
            event = json.loads(line)
            if event["seq"] < start_seq:
                continue
            yield event
            sent += 1
            if max_events is not None and sent >= max_events:
                return


def catch_up_events(start_seq: int) -> list[dict]:
    """State at start_seq: latest price per station + sessions still running.

    A receiver joining mid-stream missed these. Re-sending the original messages
    (same seq and event_id) lets it rebuild the current state; duplicates are harmless.
    """
    prices, running = {}, {}
    with open(config.EVENT_STREAM_PATH) as f:
        for line in f:
            event = json.loads(line)
            if event["seq"] >= start_seq:     # the stream file is sorted by seq
                break
            if event["event_type"] == "price_update":
                prices[event["station_id"]] = event
            elif event["event_type"] == "session":
                if event["status"] == "start":
                    running[event["session_id"]] = event
                else:
                    running.pop(event["session_id"], None)
    return sorted([*prices.values(), *running.values()], key=lambda e: e["seq"])


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--delay", type=float, default=config.MIN_PUBLISH_DELAY_S)
    ap.add_argument("--start-seq", type=int, default=0)
    ap.add_argument("--max-events", type=int, default=config.DEV_MAX_EVENTS)
    ap.add_argument("--all", action="store_true", help="ignore --max-events, publish everything")
    ap.add_argument("--no-catch-up", action="store_true",
                    help="with --start-seq: skip re-sending prices/running sessions (plain resume)")
    args = ap.parse_args()
    if args.all:
        args.max_events = None
    if args.delay < config.MIN_PUBLISH_DELAY_S:
        raise SystemExit(f"delay must be >= {config.MIN_PUBLISH_DELAY_S}s (assignment rule)")

    catch_up = [] if args.no_catch_up or args.start_seq == 0 else catch_up_events(args.start_seq)
    if catch_up:
        n_prices = sum(e["event_type"] == "price_update" for e in catch_up)
        print(f"catch-up: {len(catch_up)} messages ({n_prices} prices, "
              f"{len(catch_up) - n_prices} running sessions)", flush=True)

    client = connect()
    info, sent = None, 0
    try:
        for event in itertools.chain(catch_up, iter_events(args.start_seq, args.max_events)):
            topic = config.topic_for(event["event_type"], event["station_id"])
            retain = event["event_type"] in config.RETAIN_TYPES
            # No wait per message: a PUBACK round trip (~0.4 s) would slow the stream ~5x.
            # paho still tracks and resends unacknowledged QoS 1 messages.
            info = client.publish(topic, messages.encode(event), qos=config.QOS, retain=retain)
            sent += 1
            if sent % 1000 == 0:
                print(f"sent {sent} (seq {event['seq']}, {event['event_ts']})", flush=True)
            time.sleep(args.delay)
        if info is not None:
            info.wait_for_publish(timeout=30)   # last message acknowledged -> all sent
        print(f"done: {sent} messages ({len(catch_up)} catch-up)", flush=True)
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
