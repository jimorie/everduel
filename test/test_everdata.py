import json
import pathlib
import pytest

import everdata.everdata as ed

TEST_DB = pathlib.Path(__file__).parent / "test.db"
TEST_DB_TABLE = "things"

# Handle all test coroutines as marked with asyncio
pytestmark = pytest.mark.asyncio


@ed.register_type("test-t")
class MyThing(ed.BaseThing):
    name = ed.prop()
    price = ed.prop(default=10, typecheck=int)


@ed.register_type("test-p")
class MyPlayer(MyThing):
    thing = ed.prop(typecheck=MyThing)
    buddy = ed.prop(typecheck="test-p")
    inventory = ed.prop(default=list, typecheck=list)


@pytest.fixture
async def thingdb():
    db = await ed.ThingDatabase.connect(TEST_DB, TEST_DB_TABLE)
    try:
        await db.execute(f"DELETE FROM {TEST_DB_TABLE}")
        await db.commit()
        yield db
    finally:
        await db.close()


async def test_save_thing(thingdb):
    thing = MyThing(thingdb)
    thing.name = "Kaka"
    thing_id = await thing.save()
    r_id, r_version, r_type, r_data = await thingdb.fetchone(
        f"SELECT * FROM {TEST_DB_TABLE} WHERE id = :id",
        id=thing_id,
    )
    assert r_id == thing_id
    assert r_version == thingdb.version
    assert r_type == thing._type
    assert json.loads(r_data) == {"name": "Kaka"}
    return thing_id


async def test_load_thing(thingdb):
    thing_id = await test_save_thing(thingdb)
    thing = await thingdb.load(thing_id)
    assert type(thing) is MyThing
    assert thing.name == "Kaka"
    return thing


async def test_update_thing(thingdb):
    thing = await test_load_thing(thingdb)
    thing.name = "Bulle"
    await thing.save()
    r_data, = await thingdb.fetchone(
        f"SELECT data FROM {TEST_DB_TABLE}"
    )
    assert json.loads(r_data) == {"name": "Bulle"}


async def test_weakref_cache(thingdb):
    assert len(thingdb._cache) == 0
    thing_id = await test_save_thing(thingdb)
    assert len(thingdb._cache) == 0
    thing = await thingdb.load(thing_id)
    assert len(thingdb._cache) == 1
    del thing
    assert len(thingdb._cache) == 0
    thing1 = await thingdb.load(thing_id)
    thing2 = await thingdb.load(thing_id)
    assert thing1 is thing2
    assert len(thingdb._cache) == 1
    del thing1
    assert len(thingdb._cache) == 1
    del thing2
    assert len(thingdb._cache) == 0


async def test_save_player(thingdb):
    player = MyPlayer(thingdb)
    player.name = "Alice"
    assert player._data == {"name": "Alice",
    }
    player_id = await player.save()
    r_id, r_version, r_type, r_data = await thingdb.fetchone(
        f"SELECT * FROM {TEST_DB_TABLE} WHERE id = :id",
        id=player_id,
    )
    assert r_id == player_id
    assert r_version == thingdb.version
    assert r_type == player._type
    assert json.loads(r_data) == {"name": "Alice"}
    return player_id


async def test_load_player(thingdb):
    player_id = await test_save_player(thingdb)
    player = await thingdb.load(player_id)
    assert type(player) is MyPlayer
    assert player.name == "Alice"
    return player


async def test_save_player_thing_inline(thingdb):
    player = await test_load_player(thingdb)
    thing = MyThing(thingdb)
    thing.name = "Bulle"
    player.thing = thing
    await player.save()
    r_data, = await thingdb.fetchone(
        f"SELECT data FROM {TEST_DB_TABLE} WHERE id = :id",
        id=player._id,
    )
    assert json.loads(r_data) == {
        "name": "Alice",
        "thing": {
            "_type": thing._type,
            "name": "Bulle",
        },
    }
    return player._id


async def test_load_player_thing_inline(thingdb):
    player_id = await test_save_player_thing_inline(thingdb)
    player = await thingdb.load(player_id)
    assert type(player.thing) is MyThing
    assert player.thing.name == "Bulle"
    return player


async def test_save_player_thing_reference(thingdb):
    player = await test_load_player_thing_inline(thingdb)
    await player.thing.save()
    await player.save()
    r_data, = await thingdb.fetchone(
        f"SELECT data FROM {TEST_DB_TABLE} WHERE id = :id",
        id=player._id,
    )
    assert json.loads(r_data) == {
        "name": "Alice",
        "thing": {
            "_id": player.thing._id,
        },
    }
    return player._id


async def test_load_player_thing_reference(thingdb):
    player_id = await test_save_player_thing_reference(thingdb)
    assert player_id not in thingdb._cache
    player = await thingdb.load(player_id)
    assert player_id in thingdb._cache
    assert player.thing._id in thingdb._cache
    assert type(player.thing) is MyThing
    assert player.thing.name == "Bulle"
    return player


async def test_default(thingdb):
    thing = MyThing(thingdb)
    assert thing.price == 10
    player = MyPlayer(thingdb)
    player.inventory.append(thing)
    assert player.inventory == [thing]


async def test_save_player_thing_inline_in_list(thingdb):
    player = await test_load_player(thingdb)
    thing = MyThing(thingdb)
    thing.name = "Saft"
    player.inventory.append(thing)
    await player.save()
    r_data, = await thingdb.fetchone(
        f"SELECT data FROM {TEST_DB_TABLE} WHERE id = :id",
        id=player._id,
    )
    assert json.loads(r_data) == {
        "name": "Alice",
        "inventory": [
            {
                "_type": thing._type,
                "name": "Saft",
            }
        ],
    }
    return player._id


async def test_load_player_thing_inline_in_list(thingdb):
    player_id = await test_save_player_thing_inline_in_list(thingdb)
    player = await thingdb.load(player_id)
    assert len(player.inventory) == 1
    assert type(player.inventory[0]) is MyThing
    assert player.inventory[0].name == "Saft"
    return player


async def test_save_player_thing_reference_in_list(thingdb):
    player = await test_load_player_thing_inline_in_list(thingdb)
    thing2 = MyThing(thingdb)
    thing2.name = "Bärs"
    player.inventory.append(thing2)
    await thing2.save()
    await player.save()
    r_data, = await thingdb.fetchone(
        f"SELECT data FROM {TEST_DB_TABLE} WHERE id = :id",
        id=player._id,
    )
    assert json.loads(r_data) == {
        "name": "Alice",
        "inventory": [
            {
                "_type": player.inventory[0]._type,
                "name": "Saft",
            },
            {
                "_id": thing2._id,
            },
        ],
    }
    return player._id


async def test_load_player_thing_reference_in_list(thingdb):
    player_id = await test_save_player_thing_reference_in_list(thingdb)
    player = await thingdb.load(player_id)
    assert len(player.inventory) == 2
    assert type(player.inventory[0]) is MyThing
    assert type(player.inventory[1]) is MyThing
    assert player.inventory[0].name == "Saft"
    assert player.inventory[1].name == "Bärs"
    assert player.inventory[0]._id is None
    assert player.inventory[1]._id is not None
    return player
