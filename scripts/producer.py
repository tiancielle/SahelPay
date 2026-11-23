#!/usr/bin/env python3
"""
SahelPay - generateur de transactions (simule l'API de paiement).

Deux modes :
  --mode file   genere un fichier JSON Lines (historique pour les Parties 2 et 3)
                python scripts/producer.py --mode file --n 50000
  --mode kafka  publie en continu dans le topic Kafka "transactions"
                python scripts/producer.py --mode kafka --rate 20

Cas reels simules :
  - ~5 % d'evenements en retard (event_time recule de 1 a 15 minutes)
  - ~2 % de doublons (meme tx_id, meme status, meme contenu)
  - scenarios de fraude injectes (ids fixes, voir constantes ci-dessous) :
      * un appareil partage par de nombreux comptes
      * des transferts circulaires A -> B -> C -> A
      * un compte collecteur qui recoit de nombreux petits virements
"""
import argparse
import heapq
import json
import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone

# ---------------------------------------------------------------- constantes
CITIES = ["Casablanca", "Rabat", "Marrakech", "Fes", "Tanger",
          "Agadir", "Oujda", "Kenitra", "Tetouan", "Meknes"]
CITY_WEIGHTS = [30, 16, 11, 8, 8, 6, 4, 6, 5, 6]

N_WALLETS = 2000                     # comptes "normaux" W-10000 .. W-11999
LATE_RATE = 0.05                     # 5 % en retard
DUP_RATE = 0.02                      # 2 % de doublons
FRAUD_RATE = 0.03                    # part des transactions issues des scenarios de fraude

# Scenarios de fraude (identifiants fixes => on pourra verifier en Partie 6)
SHARED_DEVICE = "D-SHARED01"
SHARED_DEVICE_WALLETS = [f"W-{90000 + i}" for i in range(1, 13)]        # 12 comptes, 1 appareil
CYCLE_WALLETS = ["W-91001", "W-91002", "W-91003"]                        # A -> B -> C -> A
COLLECTOR_WALLET = "W-92000"                                             # compte collecteur
COLLECTOR_SOURCES = [f"W-{92000 + i}" for i in range(1, 41)]            # 40 petits virements


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


class Generator:
    def __init__(self, merchants_path, seed=None):
        self.rng = random.Random(seed)
        self.merchants = []
        with open(merchants_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    self.merchants.append(json.loads(line))
        if not self.merchants:
            sys.exit(f"merchants.json vide ou introuvable : {merchants_path}")

        self.wallets = [f"W-{10000 + i}" for i in range(N_WALLETS)]
        self.wallet_city = {w: self.rng.choices(CITIES, CITY_WEIGHTS)[0] for w in self.wallets}
        self.wallet_device = {w: "D-" + "%06x" % self.rng.getrandbits(24) for w in self.wallets}
        for w in SHARED_DEVICE_WALLETS + CYCLE_WALLETS + [COLLECTOR_WALLET] + COLLECTOR_SOURCES:
            self.wallet_city[w] = "Casablanca"
            self.wallet_device[w] = "D-" + "%06x" % self.rng.getrandbits(24)
        for w in SHARED_DEVICE_WALLETS:
            self.wallet_device[w] = SHARED_DEVICE

        self.tx_counter = 0
        self.cycle_idx = 0

    # ----------------------------------------------------------- transactions
    def _amount(self):
        return round(min(max(self.rng.lognormvariate(4.5, 1.1), 5.0), 8000.0), 2)

    def _normal_tx(self):
        rng = self.rng
        payer = rng.choice(self.wallets)
        device = self.wallet_device[payer]
        if rng.random() < 0.05:                       # 5 % : autre appareil
            device = "D-" + "%06x" % rng.getrandbits(24)
        if rng.random() < 0.70:                       # paiement commercant
            m = rng.choice(self.merchants)
            return dict(type="P2M", payer_wallet=payer, payee_id=m["merchant_id"],
                        device_id=device, city=m["city"],
                        channel=rng.choices(["QR", "APP", "USSD"], [60, 35, 5])[0],
                        amount_mad=self._amount())
        payee = rng.choice(self.wallets)
        while payee == payer:
            payee = rng.choice(self.wallets)
        return dict(type="P2P", payer_wallet=payer, payee_id=payee, device_id=device,
                    city=self.wallet_city[payer],
                    channel=rng.choices(["APP", "USSD", "QR"], [75, 20, 5])[0],
                    amount_mad=self._amount())

    def _fraud_tx(self):
        rng = self.rng
        kind = rng.choice(["shared", "cycle", "collector"])
        if kind == "shared":
            payer = rng.choice(SHARED_DEVICE_WALLETS)
            m = rng.choice(self.merchants)
            return dict(type="P2M", payer_wallet=payer, payee_id=m["merchant_id"],
                        device_id=SHARED_DEVICE, city="Casablanca", channel="APP",
                        amount_mad=round(rng.uniform(20, 300), 2))
        if kind == "cycle":
            i = self.cycle_idx % 3
            self.cycle_idx += 1
            a, b = CYCLE_WALLETS[i], CYCLE_WALLETS[(i + 1) % 3]
            return dict(type="P2P", payer_wallet=a, payee_id=b,
                        device_id=self.wallet_device[a], city="Casablanca", channel="APP",
                        amount_mad=round(rng.uniform(900, 1100), 2))
        payer = rng.choice(COLLECTOR_SOURCES)
        return dict(type="P2P", payer_wallet=payer, payee_id=COLLECTOR_WALLET,
                    device_id=self.wallet_device[payer], city="Casablanca", channel="APP",
                    amount_mad=round(rng.uniform(10, 50), 2))

    def new_transaction(self, base_time):
        """Retourne une liste de (instant_d_arrivee, evenement) pour une transaction."""
        rng = self.rng
        self.tx_counter += 1
        tx = self._fraud_tx() if rng.random() < FRAUD_RATE else self._normal_tx()
        tx_id = f"TX-{self.tx_counter:08d}"

        # cycle de vie
        r = rng.random()
        statuses = ["INITIATED"]
        if r < 0.05:
            statuses.append("REJECTED")
        else:
            statuses.append("AUTHORIZED")
            if r < 0.10:
                statuses.append("REJECTED")
            else:
                statuses.append("COMPLETED")
                if r > 0.98:
                    statuses.append("REVERSED")

        out, t = [], base_time
        for st in statuses:
            event_time = t
            if rng.random() < LATE_RATE:              # evenement en retard
                event_time = t - timedelta(seconds=rng.uniform(60, 15 * 60))
            ev = {"tx_id": tx_id, **{k: tx[k] for k in
                  ("type", "payer_wallet", "payee_id", "device_id", "city", "channel")},
                  "status": st, "amount_mad": tx["amount_mad"], "event_time": iso(event_time)}
            out.append((t, ev))
            if rng.random() < DUP_RATE:               # doublon exact
                out.append((t + timedelta(seconds=rng.uniform(0.1, 3)), dict(ev)))
            t = t + timedelta(seconds=rng.uniform(0.5, 4))
        return out


# ---------------------------------------------------------------------- modes
def run_file(gen, n, output, start):
    est_tx = max(1, n // 3)
    mean_gap = 3.0
    if start is None:
        start = datetime.now(timezone.utc) - timedelta(seconds=est_tx * mean_gap)
    events, t = [], start
    while len(events) < int(n * 1.15):
        events.extend(gen.new_transaction(t))
        t += timedelta(seconds=gen.rng.expovariate(1 / mean_gap))
    events.sort(key=lambda x: x[0])                   # ordre d'arrivee
    events = events[:n]
    os.makedirs(os.path.dirname(os.path.abspath(output)), exist_ok=True)
    with open(output, "w", encoding="utf-8") as f:
        for _, ev in events:
            f.write(json.dumps(ev) + "\n")
    print(f"{len(events)} evenements ecrits dans {output}")


def run_kafka(gen, rate, bootstrap, topic):
    try:
        from kafka import KafkaProducer
    except ImportError:
        sys.exit("Installe le client Kafka :  pip install kafka-python")
    producer = KafkaProducer(bootstrap_servers=bootstrap,
                             key_serializer=lambda k: k.encode(),
                             value_serializer=lambda v: json.dumps(v).encode())
    avg_events_per_tx = 3.1
    tx_per_sec = rate / avg_events_per_tx
    heap, seq, sent, last_print = [], 0, 0, time.time()
    carry, last = 0.0, time.time()
    print(f"Publication dans '{topic}' sur {bootstrap} a ~{rate} evt/s (Ctrl+C pour arreter)")
    try:
        while True:
            now = time.time()
            carry += (now - last) * tx_per_sec
            last = now
            while carry >= 1:
                carry -= 1
                base = datetime.now(timezone.utc)
                for arrival, ev in gen.new_transaction(base):
                    heapq.heappush(heap, (arrival.timestamp(), seq, ev))
                    seq += 1
            while heap and heap[0][0] <= time.time():
                _, _, ev = heapq.heappop(heap)
                producer.send(topic, key=ev["tx_id"], value=ev)
                sent += 1
            if time.time() - last_print >= 5:
                print(f"{sent} evenements envoyes")
                last_print = time.time()
            time.sleep(0.02)
    except KeyboardInterrupt:
        producer.flush()
        print(f"Arret. Total envoye : {sent}")


def main():
    default_merchants = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                     "..", "data", "merchants.json")
    default_output = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  "..", "data", "transactions_history.jsonl")
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--mode", choices=["file", "kafka"], required=True)
    p.add_argument("--n", type=int, default=50000, help="mode file : nombre d'evenements")
    p.add_argument("--rate", type=float, default=20, help="mode kafka : evenements par seconde")
    p.add_argument("--output", default=default_output)
    p.add_argument("--merchants", default=default_merchants)
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="transactions")
    p.add_argument("--seed", type=int, default=None)
    a = p.parse_args()

    gen = Generator(a.merchants, a.seed)
    if a.mode == "file":
        run_file(gen, a.n, a.output, None)
    else:
        run_kafka(gen, a.rate, a.bootstrap, a.topic)


if __name__ == "__main__":
    main()
