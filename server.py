
import asyncio
import math
import os
import time
import uuid

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse

BASE_DIR = Path(__file__).resolve().parent

players = {}
connections = {}
lock = asyncio.Lock()

# 공용 임무 상태
mission = {
    "fuses": [],
    "generator": False,
    "keycard": False,
    "escaped": [],
    "monster": {"x": 0.0, "z": -4.0},
}

FUSE_POSITIONS = [(-11.0, 7.0), (11.0, 4.0), (-8.0, -10.0)]
KEYCARD_POS = (12.0, -11.0)
GENERATOR_POS = (0.0, 0.0)
EXIT_POS = (0.0, -18.0)

last_monster_attack = 0.0
last_broadcast = 0.0


def distance(x1, z1, x2, z2):
    return math.hypot(x1 - x2, z1 - z2)


def new_player(name):
    return {
        "name": name,
        "x": 0.0,
        "z": 14.0,
        "yaw": math.pi,
        "hp": 100,
        "alive": True,
        "respawn_at": 0.0,
        "last_damage": 0.0,
    }


def public_state():
    now = time.monotonic()
    monster = mission["monster"]

    return {
        "type": "state",
        "players": {
            pid: {
                "name": p["name"],
                "x": p["x"],
                "z": p["z"],
                "yaw": p["yaw"],
                "hp": p["hp"],
                "alive": p["alive"],
            }
            for pid, p in players.items()
        },
        "mission": {
            "fuses": mission["fuses"],
            "generator": mission["generator"],
            "keycard": mission["keycard"],
            "escaped": mission["escaped"],
            "fuse_positions": FUSE_POSITIONS,
            "keycard_pos": KEYCARD_POS,
            "generator_pos": GENERATOR_POS,
            "exit_pos": EXIT_POS,
        },
        "monster": {
            "x": monster["x"],
            "z": monster["z"],
        },
        "server_time": now,
    }


def near(p, pos, radius=2.5):
    return distance(p["x"], p["z"], pos[0], pos[1]) <= radius


def reset_mission():
    mission["fuses"] = []
    mission["generator"] = False
    mission["keycard"] = False
    mission["escaped"] = []
    mission["monster"] = {"x": 0.0, "z": -4.0}


async def game_loop():
    global last_monster_attack

    while True:
        await asyncio.sleep(0.1)
        now = time.monotonic()

        async with lock:
            living = [
                (pid, p) for pid, p in players.items()
                if p["alive"]
            ]

            # 괴물은 가장 가까운 생존자를 추적한다.
            if living and len(mission["escaped"]) < len(players):
                monster = mission["monster"]
                target_id, target = min(
                    living,
                    key=lambda item: distance(
                        monster["x"], monster["z"],
                        item[1]["x"], item[1]["z"]
                    )
                )

                dx = target["x"] - monster["x"]
                dz = target["z"] - monster["z"]
                d = math.hypot(dx, dz)

                if d > 0.001:
                    speed = 2.15
                    monster["x"] += dx / d * speed * 0.1
                    monster["z"] += dz / d * speed * 0.1

                if d < 1.65 and now - last_monster_attack > 1.2:
                    last_monster_attack = now
                    target["hp"] = max(0, target["hp"] - 25)
                    target["last_damage"] = now

                    if target["hp"] <= 0:
                        target["alive"] = False
                        target["respawn_at"] = now + 5.0

            # 쓰러진 플레이어는 5초 후 다시 일어난다.
            for p in players.values():
                if not p["alive"] and now >= p["respawn_at"]:
                    p["alive"] = True
                    p["hp"] = 100
                    p["x"] = 0.0
                    p["z"] = 14.0

            state = public_state()
            targets = list(connections.items())

        dead_connections = []
        for pid, ws in targets:
            try:
                await ws.send_json(state)
            except Exception:
                dead_connections.append(pid)

        if dead_connections:
            async with lock:
                for pid in dead_connections:
                    connections.pop(pid, None)
                    players.pop(pid, None)


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(game_loop())
    yield
    task.cancel()


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def index():
    return FileResponse(BASE_DIR / "index.html")


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    pid = uuid.uuid4().hex[:10]

    async with lock:
        players[pid] = new_player("생존자-" + pid[:4])
        connections[pid] = ws

    await ws.send_json({"type": "welcome", "id": pid})

    try:
        while True:
            data = await ws.receive_json()
            if not isinstance(data, dict):
                continue

            async with lock:
                p = players.get(pid)
                if p is None:
                    break

                action = data.get("type")

                if action == "name":
                    name = str(data.get("name", "생존자")).strip()
                    p["name"] = name[:16] or "생존자"
                    continue

                if action == "move":
                    if not p["alive"] or pid in mission["escaped"]:
                        continue

                    try:
                        x = float(data["x"])
                        z = float(data["z"])
                        yaw = float(data["yaw"])
                    except (KeyError, ValueError, TypeError):
                        continue

                    if not all(map(math.isfinite, [x, z, yaw])):
                        continue

                    # 서버 측 월드 경계 검사
                    p["x"] = max(-18.0, min(18.0, x))
                    p["z"] = max(-18.0, min(18.0, z))
                    p["yaw"] = yaw

                elif action == "interact":
                    if not p["alive"] or pid in mission["escaped"]:
                        continue

                    item = data.get("item")

                    if item == "fuse":
                        for i, pos in enumerate(FUSE_POSITIONS):
                            if i not in mission["fuses"] and near(p, pos):
                                mission["fuses"].append(i)
                                break

                    elif item == "generator":
                        if (
                            len(mission["fuses"]) == len(FUSE_POSITIONS)
                            and near(p, GENERATOR_POS, 3.0)
                        ):
                            mission["generator"] = True

                    elif item == "keycard":
                        if near(p, KEYCARD_POS, 2.5):
                            mission["keycard"] = True

                    elif item == "exit":
                        if (
                            mission["generator"]
                            and mission["keycard"]
                            and near(p, EXIT_POS, 3.0)
                        ):
                            mission["escaped"].append(pid)

    except (WebSocketDisconnect, ValueError, TypeError):
        pass
    finally:
        async with lock:
            connections.pop(pid, None)
            players.pop(pid, None)
