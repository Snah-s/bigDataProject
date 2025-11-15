import json
import ast
from pathlib import Path
from pymongo import MongoClient, InsertOne

# config
MONGO_URI = "mongodb://root:example@mongo:27017"
DB_NAME = "copa2024"
EVENTS_COLLECTION = "events"
TEAMS_COLLECTION = "teams"
PLAYERS_COLLECTION = "players"
MATCHES_COLLECTION = "matches"

JSON_PATH = Path("/opt/airflow/data/copa32_events.json")

LIST_COLS = [
  "location",
  "pass_end_location",
  "carry_end_location",
  "shot_end_location",
  "related_events",
]

DICT_COLS = [
  "tactics",
]


def parse_maybe_literal(value):
  """Si es string tipo '[1,2]' o '{...}' intenta convertirlo; si no, lo deja igual."""
  if isinstance(value, str):
    v = value.strip()
    if (v.startswith("[") and v.endswith("]")) or \
       (v.startswith("{") and v.endswith("}")) or \
       (v.startswith("(") and v.endswith(")")):
      try:
        return ast.literal_eval(v)
      except (SyntaxError, ValueError):
        return value
  return value


def extract_prefixed_subdoc(ev: dict, prefix: str) -> dict:
  subdoc = ev.get(prefix, {})
  prefix_str = prefix + "_"

  for k, v in ev.items():
    if k.startswith(prefix_str):
      subkey = k[len(prefix_str):]  # "pass_angle" -> "angle"
      subdoc[subkey] = parse_maybe_literal(v)

  if subdoc:
    ev[prefix] = subdoc

  return ev


def clean_event(ev: dict) -> dict:
  ev = ev.copy()

  for c in LIST_COLS:
    if c in ev:
      ev[c] = parse_maybe_literal(ev[c])

  for c in DICT_COLS:
    if c in ev:
      ev[c] = parse_maybe_literal(ev[c])

  for prefix in ["pass", "shot", "carry", "dribble", "clearance"]:
    ev = extract_prefixed_subdoc(ev, prefix)

  for id_field in ["player_id", "pass_recipient_id", "team_id", "match_id"]:
    if id_field in ev and isinstance(ev[id_field], float) and ev[id_field].is_integer():
      ev[id_field] = int(ev[id_field])

  if "id" in ev and "event_id" not in ev:
    ev["event_id"] = ev["id"]

  return ev


def main():
  if not JSON_PATH.exists():
    raise FileNotFoundError(f"No se encontró el archivo JSON en {JSON_PATH.resolve()}")

  print(f"Cargando JSON desde: {JSON_PATH.resolve()}")

  with JSON_PATH.open("r", encoding="utf-8") as f:
    data = json.load(f)

  if isinstance(data, list):
    events = data
  elif isinstance(data, dict) and "events" in data:
    events = data["events"]
  else:
    raise ValueError(
      "Formato JSON no reconocido. Se esperaba una lista de eventos "
      "o un dict con clave 'events'."
    )

  print(f"Eventos totales en el JSON: {len(events)}")

  client = MongoClient(MONGO_URI, authSource="admin")
  db = client[DB_NAME]

  col_events = db[EVENTS_COLLECTION]
  col_teams = db[TEAMS_COLLECTION]
  col_players = db[PLAYERS_COLLECTION]
  col_matches = db[MATCHES_COLLECTION]

  teams_seen = {}
  players_seen = {}
  matches_seen = {}

  col_events.drop()
  col_teams.drop()
  col_players.drop()
  col_matches.drop()
  print("Colecciones events, teams, players, matches borradas.")

  bulk_ops = []

  for ev in events:
    ev_clean = clean_event(ev)

    team_id = ev_clean.get("team_id")
    team_name = ev_clean.get("team")
    player_id = ev_clean.get("player_id")
    player_name = ev_clean.get("player")
    player_position = ev_clean.get("position")
    match_id = ev_clean.get("match_id")

    if team_id is not None and team_name:
      if team_id not in teams_seen:
        teams_seen[team_id] = {
          "name": team_name,
          "players": set(),
          "matches": set(),
        }
      tdata = teams_seen[team_id]
      if match_id is not None:
        tdata["matches"].add(match_id)
      if player_id is not None:
        tdata["players"].add(player_id)

    if player_id is not None and player_name:
      if player_id not in players_seen:
        players_seen[player_id] = {
          "name": player_name,
          "teams": {},
          "positions": set(),
          "matches": set(),
        }

      pdata = players_seen[player_id]

      if not pdata["name"]:
        pdata["name"] = player_name

      if team_id is not None and team_name:
        pdata["teams"][team_id] = team_name

      if player_position:
        pdata["positions"].add(player_position)

      if match_id is not None:
        pdata["matches"].add(match_id)

    if match_id is not None:
      if match_id not in matches_seen:
        matches_seen[match_id] = {
          "teams": {},
          "player_ids": set()
        }

      mdata = matches_seen[match_id]

      if team_id is not None and team_name:
        mdata["teams"][team_id] = team_name

      if player_id is not None:
        mdata["player_ids"].add(player_id)

    if "id" in ev_clean:
      ev_clean["event_id"] = ev_clean["id"]

    bulk_ops.append(InsertOne(ev_clean))

  if bulk_ops:
    result = col_events.bulk_write(bulk_ops, ordered=False)
    print(f"Insertados {result.inserted_count} eventos en '{EVENTS_COLLECTION}'.")
  else:
    print("No hay eventos para insertar.")

  team_docs = []
  for tid, tdata in teams_seen.items():
    doc = {
      "team_id": tid,
      "name": tdata["name"],
      "players": sorted(tdata["players"]),
      "matches": sorted(tdata["matches"]),
    }
    team_docs.append(doc)

  if team_docs:
    col_teams.insert_many(team_docs)
    print(f"Insertados {len(team_docs)} equipos en '{TEAMS_COLLECTION}'.")

  player_docs = []
  for pid, pdata in players_seen.items():
    teams_list = [
      {"team_id": tid, "name": tname}
      for tid, tname in pdata["teams"].items()
    ]

    doc = {
      "player_id": pid,
      "name": pdata["name"],
      "teams": teams_list,
      "positions": sorted(pdata["positions"]),
      "matches": sorted(pdata["matches"]),
    }
    player_docs.append(doc)

  if player_docs:
    col_players.insert_many(player_docs)
    print(f"Insertados {len(player_docs)} jugadores en '{PLAYERS_COLLECTION}'.")

  match_docs = []
  for mid, mdata in matches_seen.items():
    teams_list = [
      {"team_id": tid, "name": tname}
      for tid, tname in mdata["teams"].items()
    ]
    doc = {
      "match_id": mid,
      "teams": teams_list,
      "players": sorted(mdata["player_ids"]),
    }
    match_docs.append(doc)

  if match_docs:
    col_matches.insert_many(match_docs)
    print(f"Insertados {len(match_docs)} partidos en '{MATCHES_COLLECTION}'.")

  print("Carga a Mongo finalizada.")


if __name__ == "__main__":
  main()
