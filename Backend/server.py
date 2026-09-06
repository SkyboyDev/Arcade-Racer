import asyncio
import websockets
import json
import logging
import os

# Mute internal WebSocket connection warnings
logging.getLogger("websockets").setLevel(logging.CRITICAL)

rooms = {}
connected_clients = {}

async def broadcast_lobby(room_code):
    if room_code not in rooms: return
    room = rooms[room_code]
    players_data = {pid: {"name": p["name"], "car": p["car"], "host": (pid == room["host"])} 
                    for pid, p in room["players"].items() if p["connected"]}
    
    msg = json.dumps({"type": "lobby_update", "players": players_data, "max": room["max_players"], "host": room["host"]})
    
    # FIX: Wrapped in list() to prevent iteration crash when clients disconnect
    for ws, (r_code, p_id) in list(connected_clients.items()):
        if r_code == room_code:
            try: await ws.send(msg)
            except websockets.exceptions.ConnectionClosed: pass

async def broadcast_room_state(room_code):
    if room_code not in rooms or rooms[room_code]["state"] != "racing": return
    state_message = json.dumps({
        "type": "state",
        "players": {pid: p["state"] for pid, p in rooms[room_code]["players"].items() if p["connected"]}
    })
    
    # FIX: Wrapped in list() so the physics loop doesn't crash if a lobby tab closes!
    for ws, (r_code, p_id) in list(connected_clients.items()):
        if r_code == room_code:
            try: await ws.send(state_message)
            except websockets.exceptions.ConnectionClosed: pass

async def handler(websocket):
    try:
        async for message in websocket:
            data = json.loads(message)
            msg_type = data.get("type")

            if msg_type == "join_lobby":
                room_code = data["room"].upper()
                player_id = data["id"]
                is_creating = data.get("create", False)

                if is_creating:
                    if room_code not in rooms:
                        rooms[room_code] = {
                            "host": player_id, "max_players": int(data["max"]), "state": "lobby", "rank_counter": 1,
                            "players": {}
                        }
                    else:
                        await websocket.send(json.dumps({"type": "error", "msg": "Room already exists!"}))
                        continue

                if room_code in rooms and rooms[room_code]["state"] == "lobby":
                    if len(rooms[room_code]["players"]) < rooms[room_code]["max_players"] or player_id in rooms[room_code]["players"]:
                        
                        requested_car = data.get("car", 0)
                        taken_cars = [p["car"] for p in rooms[room_code]["players"].values()]
                        if requested_car in taken_cars:
                            for i in range(5):
                                if i not in taken_cars:
                                    requested_car = i
                                    break
                                    
                        slot = len(rooms[room_code]["players"])
                        rooms[room_code]["players"][player_id] = {
                            "name": data["name"], "car": requested_car, "slot": slot, 
                            "finished": False, "rank": 0, "state": {}, "connected": True,
                            "ws": websocket  
                        }
                        connected_clients[websocket] = (room_code, player_id)
                        print(f"[*] {data['name']} joined Room {room_code}")
                        await broadcast_lobby(room_code)
                    else:
                        await websocket.send(json.dumps({"type": "error", "msg": "Room is full!"}))
                else:
                    await websocket.send(json.dumps({"type": "error", "msg": "Room not found or race already started!"}))

            elif msg_type == "rejoin_race":
                room_code = data["room"]
                player_id = data["id"]
                if room_code in rooms and player_id in rooms[room_code]["players"]:
                    rooms[room_code]["players"][player_id]["connected"] = True
                    rooms[room_code]["players"][player_id]["ws"] = websocket 
                    connected_clients[websocket] = (room_code, player_id)
                    print(f"[*] {player_id} successfully reconnected to the race in Room {room_code}!")

            elif msg_type == "change_car":
                room_code = data["room"]
                player_id = data["id"]
                requested_car = data["car"]
                if room_code in rooms and rooms[room_code]["state"] == "lobby":
                    taken_cars = [p["car"] for pid, p in rooms[room_code]["players"].items() if pid != player_id]
                    if requested_car not in taken_cars:
                        rooms[room_code]["players"][player_id]["car"] = requested_car
                    await broadcast_lobby(room_code)

            elif msg_type == "start_race":
                room_code = data["room"]
                player_id = data["id"]
                if room_code in rooms and rooms[room_code]["host"] == player_id:
                    if len(rooms[room_code]["players"]) >= 2:
                        rooms[room_code]["state"] = "racing"
                        print(f"[!] Room {room_code} has started racing!")
                        slots = {pid: p["slot"] for pid, p in rooms[room_code]["players"].items()}
                        msg = json.dumps({"type": "launch_race", "slots": slots})
                        
                        # FIX: Snapshot iteration
                        for ws, (r_code, p_id) in list(connected_clients.items()):
                            if r_code == room_code:
                                try: await ws.send(msg)
                                except: pass

            elif msg_type == "update":
                room_code = data["room"]
                player_id = data["id"]
                if room_code in rooms and player_id in rooms[room_code]["players"]:
                    rooms[room_code]["players"][player_id]["state"] = data["state"]

            elif msg_type == "finish_race":
                room_code = data["room"]
                player_id = data["id"]
                if room_code in rooms and not rooms[room_code]["players"][player_id]["finished"]:
                    rooms[room_code]["players"][player_id]["finished"] = True
                    rank = rooms[room_code]["rank_counter"]
                    rooms[room_code]["players"][player_id]["rank"] = rank
                    rooms[room_code]["rank_counter"] += 1
                    
                    finish_msg = json.dumps({"type": "player_finished", "name": rooms[room_code]["players"][player_id]["name"], "rank": rank})
                    
                    # FIX: Snapshot iteration
                    for ws, (r_code, p_id) in list(connected_clients.items()):
                        if r_code == room_code:
                            try: await ws.send(finish_msg)
                            except: pass

    except websockets.exceptions.ConnectionClosed: pass
    except Exception as e: 
        print(f"[ERROR] Connection handler crashed: {e}")
    finally:
        if websocket in connected_clients:
            room_code, player_id = connected_clients[websocket]
            del connected_clients[websocket]
            
            if room_code in rooms and player_id in rooms[room_code]["players"]:
                if rooms[room_code]["players"][player_id].get("ws") == websocket:
                    rooms[room_code]["players"][player_id]["connected"] = False
                    
                    # 20-second grace period for slower live connections
                    await asyncio.sleep(20)
                    
                    if room_code in rooms and player_id in rooms[room_code]["players"]:
                        if not rooms[room_code]["players"][player_id]["connected"]:
                            was_host = (rooms[room_code]["host"] == player_id)
                            player_name = rooms[room_code]["players"][player_id]["name"]
                            del rooms[room_code]["players"][player_id]
                            print(f"[*] {player_name} left Room {room_code}")
                            
                            if not rooms[room_code]["players"]:
                                del rooms[room_code] 
                                print(f"[*] Room {room_code} closed.")
                            else:
                                if was_host: 
                                    rooms[room_code]["host"] = list(rooms[room_code]["players"].keys())[0]
                                await broadcast_lobby(room_code)

async def physics_tick():
    while True:
        try:
            for room_code in list(rooms.keys()):
                await broadcast_room_state(room_code)
        except Exception as e:
            print(f"[ERROR] Physics tick crashed: {e}")
        await asyncio.sleep(0.05)

async def main():
    print("=================================================")
    print("  Arcade Server RUNNING on Render!               ")
    print("=================================================")
    
    port = int(os.environ.get("PORT", 8765))
    server = await websockets.serve(handler, "0.0.0.0", port)
    
    await asyncio.gather(server.wait_closed(), physics_tick())

if __name__ == "__main__":
    asyncio.run(main())
