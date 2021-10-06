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
    price = ed.prop(typecheck=int)


@ed.register_type("test-p")
class MyPlayer(MyThing):
    _root = True
    thing = ed.thing()
    buddy = ed.thing(typecheck="test-p")


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
    assert json.loads(r_data) == thing._data
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
    assert json.loads(r_data) == {"_type": MyThing._type, "name": "Bulle"}


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
    assert player._data == {
        "_type": MyPlayer._type,
        "name": "Alice",
    }
    player_id = await player.save()
    r_id, r_version, r_type, r_data = await thingdb.fetchone(
        f"SELECT * FROM {TEST_DB_TABLE} WHERE id = :id",
        id=player_id,
    )
    assert r_id == player_id
    assert r_version == thingdb.version
    assert r_type == player._type
    assert json.loads(r_data) == player._data
    thing = MyThing(thingdb)
    thing.name = "Bulle"
    player.thing = thing
    assert player._data == {
        "_type": MyPlayer._type,
        "name": "Alice",
        "thing": {"_type": "test-t", "name": "Bulle"},
    }
    await thing.save()
    assert player._data == {
        "_type": MyPlayer._type,
        "name": "Alice",
        "thing": {"_id": thing._id},
    }
    return player_id

