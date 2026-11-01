import os
import asyncio
import json
import math
import random
import string
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse

rooms = {}
clients = {}

ROOM_CAPACITY = 4

FUSE_POSITIONS = [
    (-11.0, 7.0),
    (11.0, 4.0),
    (-8.0, -10.0),
]
KEYCARD_POS = (12.0, -11.0)
GENERATOR_POS = (0.0, 0.0)
EXIT_POS = (0.0, -17.0)


def distance2d(a, b):
    return math.hypot(
        float(a[0]) - float(b[0]),
        float(a[1]) - float(b[1]),
    )


def near(player, pos, radius=2.5):
    return distance2d(
        (player.get("x", 0), player.get("z", 0)),
        pos,
    ) <= radius


def clean_name(value):
    value = str(value or "").strip()
    value = "".join(
        c for c in value
        if c.isalnum() or c in " _-가-힣"
    )
    return value[:16] or "생존자"


def clean_color(value):
    allowed = {
        "#54d6ff",
        "#ff5757",
        "#a78bfa",
        "#ffd166",
        "#7ee081",
        "#f3f4f6",
    }
    value = str(value or "#54d6ff")
    return value if value in allowed else "#54d6ff"


def clean_outfit(value):
    allowed = {
        "보안요원",
        "연구원",
        "정비공",
        "탐사복",
    }
    value = str(value or "보안요원")
    return value if value in allowed else "보안요원"


def new_mission():
    return {
        "fuses": [],
        "generator": False,
        "keycard": False,
        "escaped": [],
        "monster": {
            "x": 0.0,
            "z": -4.0,
        },
        "monster_attack_at": 0.0,
        "started_at": time.time(),
        "message": "퓨즈 3개를 찾아 발전기를 가동하세요.",
    }


def make_room_code():
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"

    for _ in range(100):
        code = "".join(
            random.choice(alphabet)
            for _ in range(5)
        )

        if code not in rooms:
            return code

    raise RuntimeError("방 번호 생성 실패")


def room_for_player(pid):
    for room in rooms.values():
        if pid in room["players"]:
            return room

    return None


def room_public(room):
    return {
        "code": room["code"],
        "host": room["host"],
        "started": room["started"],
        "capacity": ROOM_CAPACITY,
        "players": [
            {
                "id": p["id"],
                "name": p["name"],
                "color": p["color"],
                "outfit": p["outfit"],
                "host": p["id"] == room["host"],
                "alive": p.get("alive", True),
                "hp": p.get("hp", 100),
                "escaped": (
                    p["id"] in room["mission"]["escaped"]
                ),
                "x": p["x"],
                "z": p["z"],
                "yaw": p.get("yaw", 0.0),
            }
            for p in room["players"].values()
        ],
        "mission": (
            room["mission"]
            if room["started"]
            else None
        ),
    }


async def send_json(ws, payload):
    try:
        await ws.send_text(
            json.dumps(payload, ensure_ascii=False)
        )
    except Exception:
        pass


async def broadcast_room(room):
    payload = {
        "type": "room_state",
        "room": room_public(room),
    }

    await asyncio.gather(
        *[
            send_json(p["ws"], payload)
            for p in list(room["players"].values())
        ],
        return_exceptions=True,
    )


async def broadcast_error(ws, message):
    await send_json(
        ws,
        {
            "type": "error",
            "message": message,
        },
    )


def remove_player(pid):
    room = room_for_player(pid)

    if not room:
        return None

    room["players"].pop(pid, None)

    if not room["players"]:
        rooms.pop(room["code"], None)
        return None

    if room["host"] == pid:
        room["host"] = next(iter(room["players"]))

    return room


async def game_loop():
    while True:
        await asyncio.sleep(0.1)
        now = time.time()

        for room in list(rooms.values()):
            if not room["started"]:
                continue

            mission = room["mission"]
            players = room["players"]

            living = [
                p for p in players.values()
                if p.get("alive", True)
                and p["id"] not in mission["escaped"]
            ]

            if living:
                monster = mission["monster"]

                target = min(
                    living,
                    key=lambda p: distance2d(
                        (p["x"], p["z"]),
                        (monster["x"], monster["z"]),
                    ),
                )

                dx = target["x"] - monster["x"]
                dz = target["z"] - monster["z"]
                dist = math.hypot(dx, dz)

                if dist > 0.01:
                    speed = (
                        1.7 if mission["generator"]
                        else 2.15
                    )

                    monster["x"] += dx / dist * speed * 0.1
                    monster["z"] += dz / dist * speed * 0.1

                for p in living:
                    monster_distance = distance2d(
                        (p["x"], p["z"]),
                        (monster["x"], monster["z"]),
                    )

                    if (
                        monster_distance < 1.45
                        and now - mission["monster_attack_at"] > 1.2
                    ):
                        p["hp"] = max(
                            0,
                            p.get("hp", 100) - 25,
                        )

                        mission["monster_attack_at"] = now

                        if p["hp"] <= 0:
                            p["alive"] = False
                            p["respawn_at"] = now + 5.0

                        break

            for p in players.values():
                if (
                    not p.get("alive", True)
                    and now >= p.get("respawn_at", now + 99)
                ):
                    p["alive"] = True
                    p["hp"] = 100
                    p["x"] = -2.0
                    p["z"] = 15.0
                    p["message"] = "의식이 돌아왔습니다."

            await broadcast_room(room)


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(game_loop())

    try:
        yield
    finally:
        task.cancel()


app = FastAPI(
    title="DEAD SECTOR",
    lifespan=lifespan,
)


@app.get("/")
async def index():
    path = os.path.join(
        os.path.dirname(__file__),
        "index.html",
    )

    return FileResponse(path)


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()

    pid = "".join(
        random.choice(string.ascii_lowercase + string.digits)
        for _ in range(10)
    )

    clients[ws] = pid

    await send_json(
        ws,
        {
            "type": "connected",
            "id": pid,
        },
    )

    try:
        while True:
            raw = await ws.receive_text()

            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                await broadcast_error(
                    ws,
                    "잘못된 요청입니다.",
                )
                continue

            action = data.get("action")
            pid = clients[ws]

            # 방 만들기
            if action == "create_room":
                if room_for_player(pid):
                    await broadcast_error(
                        ws,
                        "이미 방에 참가해 있습니다.",
                    )
                    continue

                player = {
                    "id": pid,
                    "ws": ws,
                    "name": clean_name(data.get("name")),
                    "color": clean_color(data.get("color")),
                    "outfit": clean_outfit(data.get("outfit")),
                    "x": -2.0,
                    "z": 15.0,
                    "yaw": 0.0,
                    "hp": 100,
                    "alive": True,
                    "message": "",
                }

                code = make_room_code()

                room = {
                    "code": code,
                    "host": pid,
                    "started": False,
                    "players": {
                        pid: player,
                    },
                    "mission": new_mission(),
                }

                rooms[code] = room

                await send_json(
                    ws,
                    {
                        "type": "room_created",
                        "code": code,
                    },
                )

                await broadcast_room(room)

            # 방 참가
            elif action == "join_room":
                if room_for_player(pid):
                    await broadcast_error(
                        ws,
                        "먼저 현재 방에서 나가세요.",
                    )
                    continue

                code = str(
                    data.get("code", "")
                ).strip().upper()

                room = rooms.get(code)

                if not room:
                    await broadcast_error(
                        ws,
                        "해당 방 번호가 없습니다.",
                    )
                    continue

                if room["started"]:
                    await broadcast_error(
                        ws,
                        "이미 시작된 방입니다.",
                    )
                    continue

                if len(room["players"]) >= ROOM_CAPACITY:
                    await broadcast_error(
                        ws,
                        "방이 가득 찼습니다. 최대 4명입니다.",
                    )
                    continue

                player = {
                    "id": pid,
                    "ws": ws,
                    "name": clean_name(data.get("name")),
                    "color": clean_color(data.get("color")),
                    "outfit": clean_outfit(data.get("outfit")),
                    "x": -2.0 + len(room["players"]) * 1.2,
                    "z": 15.0,
                    "yaw": 0.0,
                    "hp": 100,
                    "alive": True,
                    "message": "",
                }

                room["players"][pid] = player

                await send_json(
                    ws,
                    {
                        "type": "joined_room",
                        "code": code,
                    },
                )

                await broadcast_room(room)

            # 방장이 게임 시작
            elif action == "start_game":
                room = room_for_player(pid)

                if not room:
                    await broadcast_error(
                        ws,
                        "먼저 방을 만들어 주세요.",
                    )

                elif room["host"] != pid:
                    await broadcast_error(
                        ws,
                        "방장만 게임을 시작할 수 있습니다.",
                    )

                elif room["started"]:
                    await broadcast_error(
                        ws,
                        "게임이 이미 시작되었습니다.",
                    )

                else:
                    room["started"] = True
                    room["mission"] = new_mission()

                    for i, p in enumerate(
                        room["players"].values()
                    ):
                        p["x"] = -1.5 + i
                        p["z"] = 15.0
                        p["hp"] = 100
                        p["alive"] = True
                        p["yaw"] = 0.0

                    await broadcast_room(room)

            # 방 나가기
            elif action == "leave_room":
                room = remove_player(pid)

                if room:
                    await broadcast_room(room)

                await send_json(
                    ws,
                    {
                        "type": "left_room",
                    },
                )

            # 플레이어 이동
            elif action == "move":
                room = room_for_player(pid)

                if not room or not room["started"]:
                    continue

                p = room["players"].get(pid)

                if (
                    not p
                    or not p.get("alive", True)
                    or pid in room["mission"]["escaped"]
                ):
                    continue

                try:
                    x = float(data.get("x", p["x"]))
                    z = float(data.get("z", p["z"]))
                    yaw = float(data.get("yaw", p["yaw"]))

                    if not all(
                        math.isfinite(v)
                        for v in (x, z, yaw)
                    ):
                        continue

                except (TypeError, ValueError):
                    continue

                p["x"] = max(-18.0, min(18.0, x))
                p["z"] = max(-18.0, min(18.0, z))
                p["yaw"] = yaw

            # 미션 상호작용
            elif action == "interact":
                room = room_for_player(pid)

                if not room or not room["started"]:
                    continue

                p = room["players"].get(pid)

                if not p or not p.get("alive", True):
                    continue

                mission = room["mission"]
                item = data.get("item")

                if item == "fuse":
                    for i, pos in enumerate(FUSE_POSITIONS):
                        if (
                            i not in mission["fuses"]
                            and near(p, pos, 2.5)
                        ):
                            mission["fuses"].append(i)
                            mission["message"] = (
                                f"퓨즈 확보 "
                                f"({len(mission['fuses'])}/3)"
                            )
                            break

                elif item == "generator":
                    if (
                        len(mission["fuses"]) == 3
                        and near(p, GENERATOR_POS, 3.0)
                    ):
                        mission["generator"] = True
                        mission["message"] = (
                            "발전기 가동 완료. 키카드를 찾으세요."
                        )

                elif item == "keycard":
                    if near(p, KEYCARD_POS, 2.5):
                        mission["keycard"] = True
                        mission["message"] = (
                            "키카드 확보. 출구로 이동하세요."
                        )

                elif item == "exit":
                    if (
                        mission["generator"]
                        and mission["keycard"]
                        and near(p, EXIT_POS, 3.0)
                        and pid not in mission["escaped"]
                    ):
                        mission["escaped"].append(pid)
                        mission["message"] = (
                            f"{p['name']} 탈출 성공!"
                        )

                await broadcast_room(room)

            # 방장이 대기실로 돌아가기
            elif action == "restart_room":
                room = room_for_player(pid)

                if not room:
                    await broadcast_error(
                        ws,
                        "방에 참가하지 않았습니다.",
                    )

                elif room["host"] != pid:
                    await broadcast_error(
                        ws,
                        "방장만 다시 시작할 수 있습니다.",
                    )

                else:
                    room["started"] = False
                    room["mission"] = new_mission()
                    await broadcast_room(room)

    except WebSocketDisconnect:
        pass

    except Exception as exc:
        print("WebSocket error:", repr(exc))

    finally:
        pid = clients.pop(ws, None)

        if pid:
            room = remove_player(pid)

            if room:
                await broadcast_room(room)
